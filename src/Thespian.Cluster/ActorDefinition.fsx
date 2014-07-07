//Prelude
//Evaluate in fsi before all else
#r "bin/debug/Nessos.Thespian.dll"
#r "bin/debug/Nessos.Thespian.Remote.dll"
#r "../Nessos.MBrace.ImemDb/bin/debug/Nessos.MBrace.ImemDb.dll"
#r "../Nessos.MBrace.Utils/bin/debug/Nessos.MBrace.Utils.dll"

#load "BehaviorExtensions.fsx"
#load "ReliableActorRef.fsx"
#load "ReliableReply.fsx"
#load "ActorExtensions.fsx"
open ReliableActorRef
open BehaviorExtensions
open BehaviorExtensions.FSM
open ActorExtensions
open ReliableReply

open System
open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Remote.TcpProtocol.Unidirectional
open Nessos.MBrace.ImemDb
open Nessos.MBrace.Utils
open Nessos.MBrace.Utils.Reversible

exception NodeNotInClusterException of string
exception ClusterInitializedException of string

exception DefinitionResolutionException of string
exception CyclicDefininitionDependency of string
exception InvalidActivationStrategy of string

exception ActivationFailureException of string * exn
exception PartialActivationException of string * exn

exception OutOfNodesException of string

exception SystemCorruptionException of string * exn
exception SystemFailureException of string

type ClusterConfiguration = {
    ClusterId: ClusterId
    Nodes: ActorRef<NodeManager> []
    ReplicationFactor: int
    FailoverFactor: int
} with
    static member New(clusterId: ClusterId) = {
        ClusterId = clusterId
        Nodes = Array.empty
        ReplicationFactor = 0
        FailoverFactor = 0
    }

and NodeManager =
    //activationResults = Activate(ch, activationRef, config, dependencyActivations)
    //Throws
    //KeyNotFoundException => defPath unknown in node
    //PartialActivationException => failure in activation could not be recovered to full
    //ActivationFailureException => activation failed
    | Activate of IReplyChannel<ActivationResult[]> * ActivationReference * ActivationConfiguration * (ClusterActivation [])
    //DeActivate(ch, activationRef, throwIfDefPathNotExists)
    //Throws
    //(throwIfDefPathNotExists => KeyNotFoundException) => defPath not found in node
    //e => exception e thrown in deactivation
    | DeActivate of IReplyChannel<unit> * ActivationReference * bool
    //newClusterStateLogger = AssumeAltMaster(ch, currentClusterState)
    //Throws
    //InvalidOperationException => already a master node
    | AssumeAltMaster of IReplyChannel<ActorRef<ClusterStateLogger>> * ClusterState
    //NotifyDependencyLost(dependant, dependency)
    | NotifyDependencyLost of DefinitionPath * ActivationReference
    //NotifyDependencyRecovered(dependant, dependency)
    | NotifyDependencyRecovered of DefinitionPath * ActivationReference
    | TriggerSystemFault
    | MasterNodeLoss
    | AttachToCluster of Cluster
    | FiniCluster of ClusterId
    | DetachFromCluster
    | UpdateAltMasters of Address []
    //Initialize a cluster
    //altMasterAddresses = InitCluster(ch, clusterConfiguration)
    //Throws
    //ClusterInitializedException => cluster already initialized
    | InitCluster of IReplyChannel<Address[]> * ClusterConfiguration
    with
        static member ActivateRevAsync(actorRef: ActorRef<NodeManager>, activationRef: ActivationReference, configuration: ActivationConfiguration, dependencyActivations: ClusterActivation[]) =
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            //FaultPoint
            //MessageHandlingException KeyNotFoundException => definition.Path not found in node;; do not handle allow to fail
            //MessageHandlingException PartialActivationException => failed to properly recover an activation failure;; do not handle allow to fail
            //MessageHandlingException ActivationFailureException => failed to activate definition.Path;; do not handle allow to fail
            //FailureException => Actor or Node level failure ;; do not handle allow to fail
            let computation = actorRef <!- fun ch -> Activate(ch, activationRef, configuration, dependencyActivations)
            let recovery (activated: ActivationResult[]) = async {
                for activationResult in activated |> Array.toList |> List.rev do
                    try
                        //FaultPoint
                        //MessageHandlingException => some exception thrown by deactivation;; allow to fail
                        //FailureException => Actor or Node level failure;; TRIGGER NODE FAILURE??? //TODO!!! Check on how to trigger node failure here
                        do! actorRef <!- fun ch -> DeActivate(ch, activationResult.ActivationReference, false)
                    with FailureException _ ->
                        //TODO!!! Perhaps log failure
                        ()
            }
            RevAsync.FromAsync(computation, recovery)

and ActivationConfiguration = {
    Specification: Map<string, Type>
    Values: Map<string, obj>
    NestedConfigurations: Map<string, ActivationConfiguration>
}

and IActivationStrategy = ///NOTE!!! Implement these properly
    abstract GetActivationNodesAsync: ClusterState * Definition -> Async<ActorRef<NodeManager> list>

and Collocated() =
    interface IActivationStrategy with
        member __.GetActivationNodesAsync(_, _) = async { return [] }

and NodeFaultReActivation(failedNode: ActorRef<NodeManager>) =
    interface IActivationStrategy with
        member __.GetActivationNodesAsync(_, _) = async { return [] }

and ActivationStrategyRegistry() =
    static let registry = dict Seq.empty

    static member GetActivationStrategy<'T>() : IActivationStrategy = ActivationStrategyRegistry.GetActivationStrategy(typeof<'T>)
    static member GetActivationStrategy(t : Type) : IActivationStrategy = registry.[t]

    static member Register<'T when 'T :> IActivationStrategy>(activationStrategy : 'T) = registry.Add(typeof<'T>, activationStrategy :> IActivationStrategy)

and ActivationReference = {
    Definition: DefinitionPath
    InstanceId: int
} with
    static member DefaultInstanceId = 0

//TODO!!! Examine the equality semantics of this
and ActivationRecord = {
    Definition: DefinitionPath
    Instance: int
    Configuration: ActivationConfiguration
    ActivationStrategy: IActivationStrategy
} with
    member ar.IsCollocated =
        match ar.ActivationStrategy with
        | :? Collocated -> true
        | _ -> false

and ActivationResult = {
    ActivationReference: ActivationReference
    Ref: ActorRef
    Configuration: ActivationConfiguration
}

and NodeLossAction = DoNothingOnNodeLoss | ReActivateOnNodeLoss

and [<AbstractClass>] Definition(parent: DefinitionPath) =
    abstract Name: string
    abstract Configuration: ActivationConfiguration
    abstract Dependencies: ActivationRecord list
    abstract ActivateAsync: int * ActivationConfiguration -> RevAsync<Activation>

    member def.Parent = parent
    abstract Path: DefinitionPath
    default def.Path = parent/def.Name

    abstract OnNodeLoss: ActorRef<NodeManager> -> NodeLossAction
    default __.OnNodeLoss _ = ReActivateOnNodeLoss

    abstract OnDependencyLoss: ActivationReference -> Async<unit>
    default __.OnDependencyLoss _ = async.Zero()

    abstract OnDependencyLossRecovery: ActivationReference -> Async<unit>
    default __.OnDependencyLossRecovery _ = async.Zero()

    override def.Equals(other: obj) =
        equalsOn (fun (def': Definition) -> def'.Path) def other

    override def.GetHashCode() = def |> hashOn (fun def' -> def'.Path)

    interface IComparable with
        member def.CompareTo(other: obj) =
            compareOn (fun (def': Definition) -> def'.Path) def other

and [<AbstractClass>] Activation(instanceId: int, definition: Definition) =
    
    member __.InstanceId = instanceId

    member __.Definition = definition

    member __.ActivationReference = { Definition = definition.Path; InstanceId = instanceId }

    abstract ActivationResults: seq<ActivationResult>

    abstract DeactivateAsync: unit -> Async<unit>
    abstract Deactivate: unit -> unit
    override a.Deactivate() = Async.RunSynchronously <| a.DeactivateAsync()

    interface IDisposable with
        member a.Dispose() = a.Deactivate()

and NodeRegistry private (currentNode: ActorRef<NodeManager>) =
    let registry = Atom.atom (Table.create <@ fun (ca: ClusterActivation) -> ca.Key @>)

    static let mutable instance = Unchecked.defaultof<NodeRegistry>

    static member Init(currentNode: ActorRef<NodeManager>) =
        instance <- new NodeRegistry(currentNode)

    static member Registry = instance

    member __.RegisterBatch(registrations: #seq<ClusterActivation>) =
        Atom.swap registry (ClusterActivation.RegisterBatch registrations)

    member reg.RegisterBatchLocal(registrations: #seq<ActivationResult>) =
        registrations |> Seq.map (fun { ActivationReference = activationRef; Ref = actorRef; Configuration = config } -> 
            { NodeManager = currentNode; ActivationReference = activationRef; Configuration = config; ActorRef = actorRef }
        ) |> reg.RegisterBatch

    member reg.Register(registration: ClusterActivation) =
        reg.RegisterBatch(Seq.singleton registration)

    member reg.RegisterLocal { ActivationReference = activationRef; Ref = actorRef; Configuration = config } = 
        reg.Register { NodeManager = currentNode; ActivationReference = activationRef; Configuration = config; ActorRef = actorRef }

    member __.UnRegister(node: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Atom.swap registry (ClusterActivation.UnRegister node activationRef)

    member reg.UnRegisterLocal(activatinoRef: ActivationReference) = reg.UnRegister(currentNode, activatinoRef)

    member __.Locals = 
        Query.from registry.Value
        |> Query.where <@ fun ca -> ca.NodeManager = currentNode @>
        |> Query.toSeq

    member __.Clear() = Atom.swap registry (fun _ -> Table.create <@ fun (ca: ClusterActivation) -> ca.Key @>)

    member __.Resolve(activationRef: ActivationReference) = ClusterActivation.Resolve activationRef registry.Value

    member __.ResolveExact<'T>(activationRef: ActivationReference) = ClusterActivation.ResolveExact<'T> activationRef registry.Value
        
    member __.ResolveLocal(activationRef: ActivationReference) =
        Query.from registry.Value
        |> Query.where <@ fun ca -> ca.ActivationReference.Definition.IsDescendantOf(activationRef.Definition) 
                                    && ca.ActivationReference.InstanceId = activationRef.InstanceId 
                                    && ca.NodeManager = currentNode @>
        |> Query.toSeq
        |> Seq.map (fun ca -> { ActivationReference = ca.ActivationReference; Configuration = ca.Configuration; Ref = ca.ActorRef })

    member __.TryResolveLocalExact<'T>(activationRef: ActivationReference) =
        Query.from registry.Value
        |> Query.where <@ fun ca -> ca.ActivationReference.Definition = activationRef.Definition 
                                    && ca.NodeManager = currentNode 
                                    && ca.ActivationReference.InstanceId = activationRef.InstanceId @>
        |> Query.toSeq
        |> Seq.map (fun ca -> ca.ActorRef :?> ActorRef<'T>)
        |> Seq.tryHead


and [<AbstractClass>] ActorDefinition<'T>(parent: DefinitionPath) =
    inherit Definition(parent)
    
    abstract Behavior: ActivationConfiguration -> Actor<'T> -> Async<unit> 

    abstract PublishProtocols: IProtocolConfiguration list
    override __.PublishProtocols = [new Unidirectional.UTcp()]
        
    override def.ActivateAsync(instanceId: int, configuration: ActivationConfiguration) = 
        RevAsync.FromAsync(
            async {
                //TODO!!! Merge configuration
                //TODO!!! Add event handler
                let actor = Actor.bind (def.Behavior configuration)
                            |> Actor.rename def.Name
                            |> Actor.publish def.PublishProtocols
                            |> Actor.start
            
                let activationRef = { Definition = def.Path; InstanceId = instanceId }
                do NodeRegistry.Registry.RegisterLocal { ActivationReference = activationRef; Ref = actor.Ref; Configuration = configuration }

                return new ActorActivation<'T>(instanceId, actor, def, configuration) :> Activation
            },
            fun actorActivation -> actorActivation.DeactivateAsync()) ///TODO!!! Handle exceptions

and ActorActivation<'T>(instanceId: int, actor: Actor<'T>, definition: ActorDefinition<'T>, configuration: ActivationConfiguration) =
    inherit Activation(instanceId, definition)

    let disposable = actor :> IDisposable

    override __.ActivationResults = Seq.singleton { 
        ActivationReference = { Definition = definition.Path; InstanceId = instanceId }; Ref = actor.Ref; Configuration = configuration }

    override __.Deactivate() = 
        NodeRegistry.Registry.UnRegisterLocal { Definition = definition.Path; InstanceId = instanceId }
        disposable.Dispose()

    override a.DeactivateAsync() = async { return a.Deactivate() }
    
    
and [<AbstractClass>] GroupDefinition(parent: DefinitionPath) =
    inherit Definition(parent)

    abstract Components: Definition list

    override def.Dependencies = 
        def.Components 
        |> List.map (fun comp -> comp.Dependencies)  
        |> List.concat

    override def.ActivateAsync(instanceId: int, configuration: ActivationConfiguration) = 
        revasync {
            let! activations =
                def.Components |> List.chooseRevAsync (fun compDef -> revasync {
                    let isNotActivated = NodeRegistry.Registry.ResolveLocal { Definition = compDef.Path; InstanceId = instanceId } |> Seq.isEmpty
                    if isNotActivated then
                        let! activation = compDef.ActivateAsync(instanceId, configuration)
                        return Some activation
                    else return None
                })

            let groupActivation = new GroupActivation(instanceId, activations, def, configuration) :> Activation

            return groupActivation
        }

and GroupActivation(instanceId: int, activations: seq<Activation>, definition: GroupDefinition, configuration: ActivationConfiguration) =
    inherit Activation(instanceId, definition)

    override __.ActivationResults = activations |> Seq.collect (fun activation -> activation.ActivationResults)

    override __.Deactivate() = for activation in activations do activation.Deactivate()
    override __.DeactivateAsync() = async { for activation in activations do do! activation.DeactivateAsync() }


and [<StructuralEquality; StructuralComparison>] DefinitionPath =
    | EmpComponent
    | NamedComponent of string * DefinitionPath
    | LiteralComponent of Definition
    with
        member path.Head =
            match path with
            | EmpComponent -> None
            | NamedComponent(defName, _) -> Some(Choice1Of2 defName)
            | LiteralComponent def -> Some(Choice2Of2 def)

        member path.TryGetHeadDefinition(definitionRegistry: DefinitionRegistry) =
            match path.Head with
            | Some(Choice1Of2 definitionName) -> definitionRegistry.TryResolve(definitionName)
            | Some(Choice2Of2 definition) -> Some definition
            | None -> None

        member path.GetHeadDefinition(definitionRegistry: DefinitionRegistry) =
            match path.TryGetHeadDefinition(definitionRegistry) with
            | Some definition -> definition
            | None -> raise (DefinitionResolutionException "Unable to resolve definition path")

        member path.IsDisjointFrom(other: DefinitionPath) =
            path.Head <> other.Head

        member path.Append(path': DefinitionPath) =
            match path with
            | EmpComponent -> path'
            | NamedComponent(name, path'') -> NamedComponent(name, path''.Append(path'))
            | LiteralComponent _ -> invalidOp "Cannot append to a literal definition path component."

        member path.IsAncestorOf(path': DefinitionPath) =
            match path, path' with
            | NamedComponent(name, rest), NamedComponent(name', rest') when name = name' -> rest.IsAncestorOf(rest')
            | EmpComponent, _ -> true
            | _ -> path = path'

        member path.IsDescendantOf(path': DefinitionPath) = path'.IsAncestorOf(path)

        member path.Append(name: string) = path.Append(NamedComponent(name, EmpComponent))

        member path.Append(definition: Definition) = path.Append(LiteralComponent definition)

        member path.Prepend(path': DefinitionPath) = path'.Append(path)

        member path.Prepend(name: string) = path.Prepend(NamedComponent(name, EmpComponent))

        override path.ToString() =
            match path with
            | EmpComponent -> ""
            | NamedComponent(name, path') -> sprintf "/%s%O" name path'
            | LiteralComponent def -> sprintf "/%s" (def.GetType().Name)

        static member (/) (path: DefinitionPath, name: string) = path.Append(name)
        static member (/) (name: string, path: DefinitionPath) = path.Prepend(name)
        static member (/) (path: DefinitionPath, path': DefinitionPath) = path.Append(path')

        static member (/) (path: DefinitionPath, definition: Definition) = path.Append(definition)

and DefinitionRegistry = {
    Registrations: Map<string, Definition>
} with
    member reg.Register(definition: Definition) =
        { reg with Registrations = reg.Registrations |> Map.add definition.Name definition }

    member reg.TryResolve(defName: string) = reg.Registrations.TryFind defName

    member reg.TryResolve(path: DefinitionPath) =
        let rec resolve path (currentDef: Definition) =
            match path, currentDef with
            | EmpComponent, _ -> Some currentDef
            | NamedComponent(defName, unresolved), (:? GroupDefinition as group) ->
                match group.Components |> List.tryFind (fun comp -> defName = comp.Name) with
                | Some def -> resolve unresolved def
                | None -> None
            | NamedComponent _, _ ->
                raise (DefinitionResolutionException "Definition path component is not a group definition")
            | LiteralComponent comp, _ -> 
                raise (DefinitionResolutionException "Literal definition components not allowed in definition registry path resolution.")
            
        match path with
        | EmpComponent -> None
        | NamedComponent(defName, unresolved) ->
            match reg.Registrations.TryFind defName with
            | Some def -> resolve unresolved def
            | None -> None
        | LiteralComponent _ ->
            raise (DefinitionResolutionException "Literal definition components not allowed in definition registry path resolution.")

    member reg.Resolve(path: DefinitionPath) =
        match reg.TryResolve path with
        | Some def -> def
        | None -> raise (new System.Collections.Generic.KeyNotFoundException(sprintf "No definition found by path %O" path))

and Cluster = {
    //Invariant:
    //Master \not\in AltMasters
    Master: Address
    AltMasters: Address list
} with //TODO!!! Extract these to a combinator
    member cluster.ClusterManager : ReliableActorRef<ClusterManager> =
        ActorRef.fromUri <| sprintf "utcp:://%O/*/clusterManager" cluster.Master
        |> ReliableActorRef.FromRef

    member cluster.MasterNode : ReliableActorRef<NodeManager> =
        ActorRef.fromUri <| sprintf "utcp:://%O/*/nodeManager" cluster.Master
        |> ReliableActorRef.FromRef

    member cluster.ClusterStateLoggers : ReliableActorRef<ClusterStateLogger> list =
        cluster.AltMasters |> List.map (fun address ->
            ActorRef.fromUri <| sprintf "utcp://%O/*/clusterStateLogger" address
            |> ReliableActorRef.FromRef)

and ClusterId = string

and ManagedCluster = {
    ClusterManager: Actor<ClusterManager>
    AltMasters: Address list
} with
    member cluster.ClusterStateLoggers =
        cluster.AltMasters |> List.map (fun address ->
            ActorRef.fromUri <| sprintf "utcp://%O/*/clusterStateLogger" address
            |> ReliableActorRef.FromRef :> ActorRef<ClusterStateLogger>)

and NodeEventManager() =
    abstract OnInit: unit -> unit
    default __.OnInit() = ()

//So far there is no point where to invoke this
//    abstract OnFini: unit -> unit
//    default __.OnFini() = ()

    abstract OnClusterInit: unit -> Async<unit>
    default __.OnClusterInit() = async.Zero()

    abstract OnAddToCluster: unit -> Async<unit>
    default __.OnAddToCluster() = async.Zero()

    abstract OnRemoveFromCluster: unit -> Async<unit>
    default __.OnRemoveFromCluster() = async.Zero()

    abstract OnSystemFault: unit -> Async<unit>
    default __.OnSystemFault() = async.Zero()
    
and ActivationPattern = ActivationReference -> ((int * ActivationConfiguration -> RevAsync<Activation>) * Async<unit>) option

and NodeState = {
    //Invariant:
    //\exists info. ClusterInfo = Some info => Address <> info.Master
    //\forall cluster. (cluster \in ManagedClusters && Address = cluster.ClusterManager.Ref.(Id :?> TcpActorId).Address)
    //ClusterStateLogger = Some clusterStateLogger => ClusterInfo = Some info && clusterStateLogger.Ref \in info.ClusterStateLoggers
    Address: Address
    NodeRegistry: NodeRegistry
    DefinitionRegistry: DefinitionRegistry
    ActivationsMap: Map<ActivationReference, Activation>
    ClusterStateLogger: Actor<ClusterStateLogger> option
    ClusterInfo: Cluster option //Info about the cluster this node participates in
    ManagedClusters: Map<ClusterId, ManagedCluster> //The clusters this node is managing 
    EventManager: NodeEventManager
    ActivationPatterns: ActivationPattern list
} with
    member state.IsAltMaster =
        if state.ClusterInfo.IsNone then false else
        state.ClusterInfo.Value.AltMasters |> List.exists (fun altMasterAddress -> altMasterAddress = state.Address)

    member state.AltMasterIndex =
        state.ClusterInfo.Value.AltMasters |> List.findIndex (fun altMasterAddress -> altMasterAddress = state.Address)

    member state.ClusterStateLoggers =
        match state.ClusterInfo with
        | Some cluster -> cluster.ClusterStateLoggers
        | None -> []

    member state.NodeManager: ActorRef<NodeManager> =
        ActorRef.fromUri <| sprintf "utcp:://%O/*/nodeManager" state.Address

    static member New(address: Address, 
                      nodeRegistry: NodeRegistry, 
                      definitionRegistry: DefinitionRegistry, 
                      eventManager: NodeEventManager,
                      activationPatterns: ActivationPattern list) = {
        Address = address
        NodeRegistry = nodeRegistry
        DefinitionRegistry = definitionRegistry
        ActivationsMap = Map.empty
        ClusterStateLogger = None
        ClusterInfo = None
        ManagedClusters = Map.empty
        EventManager = eventManager
        ActivationPatterns = activationPatterns
    }

and ClusterActiveDefinition = {
    NodeManager: ActorRef<NodeManager>
    ActivationReference: ActivationReference
    Configuration: ActivationConfiguration
} with
    static member MakeKey(nodeManager: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Key.composite [| nodeManager; activationRef |]

    member cd.Key = ClusterActiveDefinition.MakeKey(cd.NodeManager, cd.ActivationReference)

and ClusterActivation = {
    NodeManager: ActorRef<NodeManager>
    ActivationReference: ActivationReference
    Configuration: ActivationConfiguration
    ActorRef: ActorRef
} with
    static member MakeKey(nodeManager: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Key.composite [| nodeManager; activationRef |]

    member ca.Key = ClusterActivation.MakeKey(ca.NodeManager, ca.ActivationReference)

    static member RegisterBatch (registrations: seq<ClusterActivation>) (table: Table<ClusterActivation>) =
            registrations |> Seq.fold (fun tbl activationResult ->
                Table.insert activationResult tbl
            ) table

    static member Register (registration: ClusterActivation) (table: Table<ClusterActivation>) = 
        ClusterActivation.RegisterBatch (Seq.singleton registration) table

    static member UnRegister (node: ActorRef<NodeManager>) (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        Table.remove (ClusterActivation.MakeKey(node, activationRef)) table

    static member ResolveActivation (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        Query.from table
        |> Query.where <@ fun ca -> ca.ActivationReference.Definition.IsDescendantOf(activationRef.Definition) && ca.ActivationReference.InstanceId = activationRef.InstanceId @>
        |> Query.toSeq

    static member Resolve (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        ClusterActivation.ResolveActivation activationRef table
        |> Seq.map (fun ca -> { ActivationReference = ca.ActivationReference; Ref = ca.ActorRef; Configuration = ca.Configuration })

    static member ResolveExactActivation (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        Query.from table
        |> Query.where <@ fun ca -> ca.ActivationReference = activationRef @>
        |> Query.toSeq

    static member ResolveDependants (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        Query.from table
        |> Query.where <@ fun ca -> ca.ActivationReference.Definition.IsAncestorOf activationRef.Definition 
                                    && ca.ActivationReference.InstanceId = activationRef.InstanceId @>
        |> Query.toSeq

    static member ResolveDependantsOfDefPath (defPath: DefinitionPath) (table: Table<ClusterActivation>) =
        Query.from table
        |> Query.where <@ fun ca -> ca.ActivationReference.Definition.IsAncestorOf defPath @>
        |> Query.toSeq

    static member ResolveExact<'T> (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        ClusterActivation.ResolveExactActivation activationRef table
        |> Seq.map (fun ca -> ca.ActorRef :?> ActorRef<'T>)

and ClusterNode = {
    NodeManager: ActorRef<NodeManager>
    LastHeartBeat: DateTime
}

and ClusterAltMasterNode = {
    NodeManager: ActorRef<NodeManager>
    ClusterStateLogger: ActorRef<ClusterStateLogger> //use ReliableActorRefs
}

and ClusterDb = {
    ClusterNode: Table<ClusterNode>
    ClusterAltMasterNode: Table<ClusterAltMasterNode>
    ClusterActiveDefinition: Table<ClusterActiveDefinition>
    ClusterActivation: Table<ClusterActivation>
} with
    static member Empty = {
        ClusterNode = Table.create <@ fun clusterNode -> clusterNode.NodeManager @>
        ClusterAltMasterNode = Table.create <@ fun clusterAltMasterNode -> clusterAltMasterNode.NodeManager @>
        ClusterActiveDefinition = Table.create <@ fun clusterActiveDefinition -> clusterActiveDefinition.Key @>
        ClusterActivation = Table.create <@ fun clusterActivation -> clusterActivation.Key @>
    }

    //TODO!! export/import maybe?

and ClusterState = { //TODO!! Integrate performance monitoring
    ClusterId: ClusterId
    MasterNode: ActorRef<NodeManager> //TODO!!! Make this also an in-memory ref
    Db: ClusterDb
    TimeSpanToDeathDeclaration: TimeSpan
    DefinitionRegistry: DefinitionRegistry
} with //TODO!!! Put update methods these in a module

    static member New(clusterId: ClusterId, masterNode: ActorRef<NodeManager>, timeSpanToDeathDeclaration: TimeSpan, definitionRegistry: DefinitionRegistry) = {
        ClusterId = clusterId
        MasterNode = masterNode
        Db = ClusterDb.Empty
        TimeSpanToDeathDeclaration = timeSpanToDeathDeclaration
        DefinitionRegistry = definitionRegistry
    }

    member state.Master = 
        (state.MasterNode.[UTCP].Id :?> TcpActorId).Address

    //These are all the slave nodes
    member state.Nodes =
        Query.from state.Db.ClusterNode
        |> Query.toSeq
        |> Seq.map (fun node -> node.NodeManager)

    member state.LiveNodesQueryFilter =
        Query.where <@ fun node -> (DateTime.Now - node.LastHeartBeat) < state.TimeSpanToDeathDeclaration @>

    member state.DeadNodesQueryFilter =
        Query.where <@ fun node -> (DateTime.Now - node.LastHeartBeat) > state.TimeSpanToDeathDeclaration @>

    member state.ClusterStateLoggers =
        Query.from state.Db.ClusterAltMasterNode
        |> Query.toSeq
        |> Seq.map (fun altNode -> altNode.ClusterStateLogger)

    member state.ClusterAltMasterNodes =
        Query.from state.Db.ClusterAltMasterNode
        |> Query.toSeq

    member state.AltMasterNodes =
        Query.from state.Db.ClusterAltMasterNode
        |> Query.toSeq
        |> Seq.map (fun altNode -> altNode.NodeManager)

    member state.AltMasters =
        state.ClusterStateLoggers
        |> Seq.map (fun ref -> ref.Id)
        |> Seq.choose (function :? TcpActorId as id -> Some id | _ -> None)
        |> Seq.map (fun id -> id.Address)
        |> Seq.toList

    member state.DeadNodes = 
        Query.from state.Db.ClusterNode
        |> state.DeadNodesQueryFilter
        |> Query.toSeq
        |> Seq.map (fun node -> node.NodeManager)

    member state.AddNode(nodeManager: ActorRef<NodeManager>): ClusterState =
        { state with Db = state.Db |> Database.insert <@ fun db -> db.ClusterNode @> { NodeManager = nodeManager; LastHeartBeat = DateTime.Now } }

    member state.RemoveNode(nodeManager: ActorRef<NodeManager>): ClusterState =
        { state with
            Db = state.Db |> Database.remove <@ fun db -> db.ClusterNode @> nodeManager
                          |> Database.remove <@ fun db -> db.ClusterAltMasterNode @> nodeManager
                          |> Database.delete <@ fun db -> db.ClusterActivation @> <@ fun ca -> ca.NodeManager = nodeManager @>
                          |> Database.delete <@ fun db -> db.ClusterActiveDefinition @> <@ fun cd -> cd.NodeManager = nodeManager @>
        }

    member state.SetTimeSpanToDeathDeclaration(timeSpan: TimeSpan): ClusterState =
        { state with TimeSpanToDeathDeclaration = timeSpan }

    member state.AddActivation(clusterActivation: ClusterActivation): ClusterState =
        { state with Db = state.Db |> Database.insert <@ fun db -> db.ClusterActivation @> clusterActivation }

    member state.AddActiveDefinition(clusterActiveDefinition: ClusterActiveDefinition): ClusterState =
        { state with Db = state.Db |> Database.insert <@ fun db -> db.ClusterActiveDefinition @> clusterActiveDefinition }

    member state.DeActivateDefinition(activationRef: ActivationReference): ClusterState =
        { state with Db = state.Db |> Database.delete <@ fun db -> db.ClusterActivation @> <@ fun ca -> ca.ActivationReference.Definition.IsDescendantOf activationRef.Definition 
                                                                                                        && ca.ActivationReference.InstanceId = activationRef.InstanceId @> 
                                   |> Database.delete <@ fun db -> db.ClusterActiveDefinition @> <@ fun cd -> cd.ActivationReference = activationRef @> }

    member state.UpdateHeartBeat(node: ActorRef<NodeManager>): ClusterState = 
        state.AddNode node //has the same effect

    member state.AddAltMasterNode(clusterAltMasterNode: ClusterAltMasterNode): ClusterState =
        if state.Db.ClusterNode.DataMap.ContainsKey clusterAltMasterNode.NodeManager then
            { state with Db = state.Db |> Database.insert <@ fun db -> db.ClusterAltMasterNode @> clusterAltMasterNode }
        else invalidArg "clusterAltMasterNode" "Given node is not in the cluster database."

    member state.RemoveAltMasterNode(nodeManager: ActorRef<NodeManager>): ClusterState =
        { state with Db = state.Db |> Database.remove <@ fun db -> db.ClusterAltMasterNode @> nodeManager }

and ClusterManager =
    //ActivateDefinition(ch, activationRecord)
    //Throws
    //InvalidActivationStrategy => invalid argument;; collocation strategy not supported
    //CyclicDefininitionDependency => invalid configuration 
    //OutOfNodesExceptions => unable to recover due to lack of nodes
    //KeyNotFoundException ;; from NodeManager => definition.Path not found in node
    //ActivationFailureException ;; from NodeManager => failed to activate definition.Path
    //SystemCorruptionException => system failure occurred during execution;; SYSTEM FAULT
    //SystemFailureException => Global system failure;; SYSTEM FAULT
    | ActivateDefinition of IReplyChannel<unit> * ActivationRecord
    //DeActivateDefinition(ch, activationReference)
    //Throws
    //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
    //OutOfNodesException => unable to recover due to lack of node;; SYSTEM FAULT
    //SystemFailureException => Global system failure;; SYSTEM FAULT
    | DeActivateDefinition of IReplyChannel<unit> * ActivationReference
    | RemoveNode of ActorRef<NodeManager>
    | AddNode of ActorRef<NodeManager>
    | HeartBeat of ActorRef<NodeManager>
    | KillCluster
    | HealthCheck

and ClusterStateUpdate =
    | ClusterAddNode of ActorRef<NodeManager>
    | ClusterRemoveNode of ActorRef<NodeManager>
    | ClusterSetTimeSpanToDeathDeclaration of TimeSpan
    | ClusterActivation of ClusterActivation
    | ClusterActiveDefinition of ClusterActiveDefinition
    | ClusterDeActivateDefinition of ActivationReference
    | ClusterAddAltMaster of ClusterAltMasterNode
    | ClusterRemoveAltMaster of ActorRef<NodeManager>

and ClusterUpdate = { 
    NodeInsertions: ActorRef<NodeManager> []
    NodeRemovals: ActorRef<NodeManager> []
    TimeSpanToDeath: TimeSpan option
    Activations: ClusterActivation []
    DefinitionActivations: ClusterActiveDefinition []
    DefinitionDeActivations: ActivationReference []
    AltMasterAddittions: ClusterAltMasterNode []
    AltMasterRemovals: ActorRef<NodeManager>[]
} with
    static member FromUpdates(updates: #seq<ClusterStateUpdate>) =
        {
            NodeInsertions = updates |> Seq.choose (function ClusterAddNode node -> Some node | _ -> None) |> Seq.toArray
            NodeRemovals = updates |> Seq.choose (function ClusterRemoveNode node -> Some node | _ -> None) |> Seq.toArray
            TimeSpanToDeath = updates |> Seq.tryFind (function ClusterSetTimeSpanToDeathDeclaration _ -> true | _ -> false) |> Option.map (function ClusterSetTimeSpanToDeathDeclaration t -> t | _ -> failwith "FAULT")
            Activations = updates |> Seq.choose (function ClusterActivation ca -> Some ca | _ -> None) |> Seq.toArray
            DefinitionActivations = updates |> Seq.choose (function ClusterActiveDefinition cd -> Some cd | _ -> None) |> Seq.toArray
            DefinitionDeActivations = updates |> Seq.choose (function ClusterDeActivateDefinition path -> Some path | _ -> None) |> Seq.toArray
            AltMasterAddittions = updates |> Seq.choose (function ClusterAddAltMaster am -> Some am | _ -> None) |> Seq.toArray
            AltMasterRemovals = updates |> Seq.choose (function ClusterRemoveAltMaster n -> Some n | _ -> None) |> Seq.toArray
        }

    static member Update(state: ClusterState, update: ClusterUpdate): ClusterState =
        let fold f is s = Seq.fold f s is
        state |> fold (fun s n -> s.AddNode n) update.NodeInsertions
              |> fold (fun s n -> s.RemoveNode n) update.NodeRemovals
              |> fold (fun s a -> s.AddActivation a) update.Activations
              |> fold (fun s d -> s.AddActiveDefinition d) update.DefinitionActivations
              |> fold (fun s p -> s.DeActivateDefinition p) update.DefinitionDeActivations
              |> fold (fun s n -> s.AddAltMasterNode n) update.AltMasterAddittions
              |> fold (fun s n -> s.RemoveAltMasterNode n) update.AltMasterRemovals
              |> (fun ts (s: ClusterState) -> match ts with Some t -> s.SetTimeSpanToDeathDeclaration t | None -> s) update.TimeSpanToDeath
        

and ClusterStateLogger =
    | UpdateClusterState of ClusterUpdate
    | GetClusterState of IReplyChannel<ClusterState>


let clusterStateLoggerBehavior (state: ClusterState) (msg: ClusterStateLogger) =
    async {
        match msg with
        | UpdateClusterState clusterUpdate ->
            return ClusterUpdate.Update(state, clusterUpdate)

        | GetClusterState(R reply) ->
            reply <| Value state
            return state
    }

let rec (|LiteralDefinition|_|) =
    function EmpComponent -> None
             | NamedComponent(_, LiteralDefinition def) -> Some def
             | LiteralComponent def -> Some def
             | _ -> None

let updateState (state: ClusterState) (updates: seq<ClusterStateUpdate>) =
    updates |> Seq.fold (fun (state: ClusterState) update ->
        match update with
        | ClusterAddNode node -> state.AddNode node
        | ClusterRemoveNode node -> state.RemoveNode node
        | ClusterSetTimeSpanToDeathDeclaration timeSpan -> state.SetTimeSpanToDeathDeclaration timeSpan
        | ClusterActivation ca -> state.AddActivation ca
        | ClusterActiveDefinition cd -> state.AddActiveDefinition cd
        | ClusterDeActivateDefinition defPath -> state.DeActivateDefinition defPath
        | ClusterAddAltMaster am -> state.AddAltMasterNode am
        | ClusterRemoveAltMaster n -> state.RemoveAltMasterNode n
    ) state

//TODO!!! Check if instanceid should be included in the check
let checkAcyclicDependencies (definitionRegistry: DefinitionRegistry) (defPath: DefinitionPath) =
    let rec checkAcyclicDependencies' (path: DefinitionPath list) (unchecked: DefinitionPath list) =
        match unchecked with
        | [] -> true
        | dependencyPath::rest ->
            let definition = dependencyPath.GetHeadDefinition(definitionRegistry)

            let cycleDetected =
                definition.Dependencies |> Seq.exists (fun dependency ->
                    path |> List.exists (fun definitionPath -> definitionPath.Head = dependency.Definition.Head)   
                )

            if cycleDetected then not cycleDetected
            else
                if definition.Dependencies |> Seq.isEmpty then
                    checkAcyclicDependencies' [] rest
                else
                    let uncheckedDefs = definition.Dependencies |> Seq.map (fun d -> d.Definition)
                                                                |> Seq.toList
                    checkAcyclicDependencies' (dependencyPath::path) (uncheckedDefs@rest)

    checkAcyclicDependencies' [] [defPath]

exception ClusterStateLogBroadcastException of string * ActorRef<NodeManager> list
exception ClusterStateLogFailureException of string


//Throws
//ClusterStateLogBroadcastException => some updates have failed (node failures)
//ClusterStateLogFailureException => total failure to update (all alt master node failures)
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let logClusterUpdate (state: ClusterState) clusterUpdate =
    async {
        let altMasterNodes = state.ClusterAltMasterNodes |> Seq.cache
        let! rs =
            altMasterNodes
            |> Seq.map (fun { NodeManager = node; ClusterStateLogger = logger } -> async { 
                try
                    //FaultPoint
                    //FailureException => node failure
                    do (ReliableActorRef.FromRef logger) <-- UpdateClusterState clusterUpdate 
                    return None
                with e ->
                    return Some node
            })
            |> Async.Parallel

        let failedNodes = rs |> Seq.choose id |> Seq.toList
        if failedNodes.Length = 0 then return () 
        elif failedNodes.Length = Seq.length altMasterNodes then return! Async.Raise (ClusterStateLogFailureException "Failed to replicate cluster state at least once.")
        else return! Async.Raise <| ClusterStateLogBroadcastException("Failed to replicate cluster state to some nodes.", failedNodes)
    }

//Throws:
//InavalidActivationStrategy => Collocated activation strategy not supported
//CyclicDefininitionDependency => Cyclic dependency detected between dependencies of attempted activattion
//MessageHandlingException KeyNotFoundException ;; from NodeManager => definition.Path not found in node
//MessageHandlingException PartialActivationException ;; from NodeManager => failed to properly recover an activation failure
//MessageHandlingException ActivationFailureException ;; from NodeManager => failed to activate definition.Path
//FailureException;; from NodeManager
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let rec activateDefinition (state: ClusterState) (activationRecord: ActivationRecord) =
    
    let updatedState (activations: ClusterActivation list, activeDefinitions: ClusterActiveDefinition list) (state: ClusterState) =
        { state with Db = state.Db |> List.foldBack (Database.insert <@ fun db -> db.ClusterActivation @>) activations
                                   |> List.foldBack (Database.insert <@ fun db -> db.ClusterActiveDefinition @>) activeDefinitions }

    let stateUpdates (activations: ClusterActivation list, activeDefinitions: ClusterActiveDefinition list) =
        seq {
            yield! activations |> Seq.map ClusterActivation
            yield! activeDefinitions |> Seq.map ClusterActiveDefinition
        }

    let rec activateDefinition'
                (aggregateActivations: ClusterActivation list, aggregateActiveDefinitions: ClusterActiveDefinition list) 
                { Definition = definitionPath; Instance = instanceId; Configuration = configuration; ActivationStrategy = activationStrategy } =
        revasync {
            match activationStrategy with
            | :? Collocated -> raise (InvalidActivationStrategy "ClusterManager does not accept the Collocated activation strategy.")
            | _ -> ()
            
            let isActivated = 
                aggregateActivations |> List.exists (fun { ActivationReference = activationRef } -> activationRef.Definition.IsAncestorOf(definitionPath) 
                                                                                                    && activationRef.InstanceId = instanceId)
            if not isActivated then
                if checkAcyclicDependencies state.DefinitionRegistry definitionPath then
                    let definition = definitionPath.GetHeadDefinition(state.DefinitionRegistry)

                    //Activate non-collocated dependencies
                    let externalDependencies = 
                        definition.Dependencies |> Seq.filter (fun dep -> not dep.IsCollocated)
                                                |> Seq.distinct
                                                |> Seq.toList

                    let! aggregateActivations', aggregateActiveDefinitions' = 
                        externalDependencies |> List.foldRevAsync activateDefinition' (aggregateActivations, aggregateActiveDefinitions)

                    //after dependencies have been sucessfully activated
                    //activate this definition
                    let tempState = updatedState (aggregateActivations', aggregateActiveDefinitions') state
                    let! activationNodes = RevAsync.FromAsync <| activationStrategy.GetActivationNodesAsync(tempState, definition)
                    
                    return!
                        activationNodes |> List.foldRevAsync (fun (aggregateActivations, aggregateActiveDefinitions) activationNode -> revasync {
                            let dependencyActivations = aggregateActivations |> List.toArray

                            let activationNode = ReliableActorRef.FromRef activationNode
                            let activationRef = { Definition = definition.Path; InstanceId = instanceId }
                            //FaultPoint
                            //MessageHandlingException KeyNotFoundException => definition.Path not found in node;; do not handle allow to fail
                            //MessageHandlingException PartialActivationException => failed to properly recover an activation failure;; do not handle allow to fail
                            //MessageHandlingException ActivationFailureException => failed to activate definition.Path;; do not handle allow to fail
                            //FailureException => Actor or Node level failure ;; do not handle allow to fail
                            let! activationResults = NodeManager.ActivateRevAsync(activationNode, activationRef, configuration, dependencyActivations)

                            return 
                                activationResults |> Seq.fold (fun activations { ActivationReference = activationRef'; Ref = actorRef } -> 
                                    { 
                                        NodeManager = activationNode
                                        ActivationReference = activationRef'
                                        Configuration = activationRecord.Configuration
                                        ActorRef = actorRef 
                                    } :: activations) aggregateActivations
                                |> Seq.toList,
                                { 
                                    NodeManager = activationNode
                                    ActivationReference = activationRef
                                    Configuration = configuration
                                } :: aggregateActiveDefinitions
                        }) (aggregateActivations', aggregateActiveDefinitions')
                else
                    //Cyclic dependencies detected 
                    return raise (CyclicDefininitionDependency "Definition dependency cycle detected.")
            else
                //The definition is already activated 
                return aggregateActivations, aggregateActiveDefinitions
        }

    revasync {
        let! activations = activateDefinition' ([], []) activationRecord
        
        return stateUpdates activations
    }

//Throws
//CompensationFault => Failure in rollback ;; Compensations must be exception safe;; SYSTEM FAULT
//FinalizationFault => Failure in finalization;; Finalizations must be exception safe;; SYSTEM FAULT
//InvalidActivationStrategy => invalid argument ;; reversal successfull;; exception will be replied
//CyclicDefininitionDependency => invalid configuration ;; reversal successfull
//MessageHandlingException KeyNotFoundException ;; from NodeManager => definition.Path not found in node
//MessageHandlingException PartialActivationException ;; from NodeManager => failed to properly recover an activation failure;; SYSTEM FAULT
//MessageHandlingException ActivationFailureException ;; from NodeManager => failed to activate definition.Path
//SystemCorruptionException => system inconsistency;; SYSTEM FAULT
//OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and activationLoop (ctx: BehaviorContext<_>) (state: ClusterState) activationRecord = 
    let rec activationLoop' (state: ClusterState) updates = async {
        try
            //FaultPoint
            //CompensationFault => Failure in rollback ;; Compensations must be exception safe;; fall through;; SYSTEM FAULT
            //FinalizationFault => Failure in finalization;; Finalizations must be exception safe;; fall through;; SYSTEM FAULT
            //InvalidActivationStrategy => invalid argument ;; reversal successfull;; allow to fail;; exception will be replied
            //CyclicDefininitionDependency => invalid configuration ;; reversal successfull;; allow to fail;; exception will be replied
            //MessageHandlingException KeyNotFoundException ;; from NodeManager => definition.Path not found in node;; allow to fail;; exception will be replied
            //MessageHandlingException PartialActivationException ;; from NodeManager => failed to properly recover an activation failure;; fall through;; SYSTEM FAULT
            //MessageHandlingException ActivationFailureException ;; from NodeManager => failed to activate definition.Path;; allow to fail;; exception will be replied
            //FailureException;; from a NodeManager => node failure ;; trigger node recovery ;; retry activation
            return! RevAsync.ToAsync <| activateDefinition state activationRecord 
        with FailureException(_, (:? ActorRef<NodeManager> as nodeManager)) ->
            //FaultPoint
            //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
            //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
            let! updates' = removeNode ctx state updates nodeManager

            let state' = updateState state updates'
            return! activationLoop' state' updates'
    }

    activationLoop' state Seq.empty

/// Recovers the activation of a definition of a lost node.
//Throws
//SystemCorruptionException => unexpected reactivation failure;; SYSTEM FAULT
//OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and recoverDefinitionNodeLoss (ctx: BehaviorContext<_>) (lostNode : ActorRef<NodeManager>) (state: ClusterState) (updates: seq<ClusterStateUpdate>)
    (definition: Definition, instanceId: int, configuration: ActivationConfiguration) =

    let raiseSystemCorruption e =
        Async.Raise <| SystemCorruptionException(sprintf "System corruption during recovery of definition %O in lost node %O" definition.Path lostNode, e)

    async {
        //get dependants of definition
        let dependantActivations = 
            ClusterActivation.ResolveDependantsOfDefPath definition.Path state.Db.ClusterActivation
            |> Seq.groupBy (fun activation -> activation.NodeManager)
            |> Seq.cache

        let activationRef = { Definition = definition.Path; InstanceId = instanceId }

        //notify dependants of dependency loss
        do! dependantActivations
            |> Seq.map (fun (nodeManager, dependantActivations) -> async {
                //TODO!!! BATCH THIS LOOP
                for dependantActivation in dependantActivations do
                    try
                        //FaultPoint
                        //FailureException => node failure ;; do nothing, allow for later detectection
                        (ReliableActorRef.FromRef nodeManager) <-- NotifyDependencyLost(dependantActivation.ActivationReference.Definition, activationRef)
                    with (FailureException _) as e ->
                        ctx.LogWarning(sprintf "Failed to NotifyDependencyLost: %A" e)
            })
            |> Async.Parallel
            |> Async.Ignore

        let! updates = async {
            match definition.OnNodeLoss lostNode with
            | ReActivateOnNodeLoss ->
                let reActivationStrategy = new NodeFaultReActivation(lostNode) :> IActivationStrategy
                //Throws
                //SystemCorruptionException => unexpected reactivation failure;; SYSTEM FAULT
                try
                    //FaultPoint
                    //CompensationFault => Failure in rollback ;; Compensations must be exception safe;; allow to fail;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //FinalizationFault => Failure in finalization;; Finalizations must be exception safe;; allow to fail;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //InvalidActivationStrategy => invalid argument ;; reversal successfull;; activation strategy should be NodeFaultReActivation;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //CyclicDefininitionDependency => invalid configuration ;; reversal successfull;; activation under recovery should not have cyclic dependencies;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //MessageHandlingException KeyNotFoundException ;; from NodeManager => definition.Path not found in node;; this is a reactivation, definition was known before;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //MessageHandlingException PartialActivationException ;; from NodeManager => failed to properly recover an activation failure;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //MessageHandlingException ActivationFailureException ;; from NodeManager => failed to activate definition.Path;; this is reactivation, it should not fail;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
                    //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
                    let! updates' = activationLoop ctx state { 
                        Definition = definition.Path; 
                        Instance = instanceId; 
                        Configuration = configuration; 
                        ActivationStrategy = reActivationStrategy 
                    }

                    return Seq.append updates updates'
                with MessageHandlingException2(:? System.Collections.Generic.KeyNotFoundException as e) -> return! raiseSystemCorruption e
                    | MessageHandlingException2((ActivationFailureException _ | PartialActivationException _) as e) -> return! raiseSystemCorruption e
                    | SystemCorruptionException _
                    | OutOfNodesException _ as e -> return! Async.Raise e
                    | e ->  return! raiseSystemCorruption e

            | DoNothingOnNodeLoss ->
                return updates
        }

        //notify dependants of dependency recovery
        do! dependantActivations
            |> Seq.map (fun (nodeManager, dependantActivations) -> async {
                //TODO!!! BATCH THIS LOOP
                for dependantActivation in dependantActivations do
                    try
                        //FaultPoint
                        //FailureException => node failure ;; do nothing, allow for later detectection
                        (ReliableActorRef.FromRef nodeManager) <-- NotifyDependencyRecovered(dependantActivation.ActivationReference.Definition, activationRef)
                    with (FailureException _) as e ->
                        ctx.LogWarning(sprintf "Failed to NotifyDependencyRecovered: %A" e)
                        ()
            })
            |> Async.Parallel
            |> Async.Ignore

        return updates
    }

//Throws
//KeyNotFoundException => no nodes available
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and selectNewClusterStateLogger (clusterState: ClusterState) =
    let loggerNodes =
        Query.from clusterState.Db.ClusterAltMasterNode
        |> Query.toSeq
        |> Seq.map (fun node -> node.NodeManager)
        |> Set.ofSeq

    Query.from clusterState.Db.ClusterNode
    |> Query.where <@ fun node -> not(Set.contains node.NodeManager loggerNodes) @>
    |> Query.toSeq
    |> Seq.map (fun node -> node.NodeManager)
    |> Seq.head

/// If the node is an AltMaster then it proceeds with AltMaster recovery
/// by selecting a new AltMaster node. If the node is not an AltMaster node then it does nothing
/// and returns the state unchanged.
//Throws
//MessageHandlingException InvalidOperationException => already a master node
//SystemCorruptionException => system inconsistency;; SYSTEM FAULT
//OutOfNodesExceptions => unalbe to recover due to lack of node;; SYSTEM FAULT
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and recoverAltMaster (ctx: BehaviorContext<_>) (state: ClusterState) (updates: seq<ClusterStateUpdate>) (node: ActorRef<NodeManager>) =
    let rec recoveryLoop (state: ClusterState) (updates: seq<ClusterStateUpdate>) = 
        async {
            //FaultPoint
            //OutOfNodesExceptions => no more nodes;; cannot recover;; propagate;; SYSTEM FAULT
            let! newAltMasterNode = async {
                try
                    //FaultPoint
                    //KeyNotFoundException => no nodes available => reraise to OutOfNodesExceptions
                    return ReliableActorRef.FromRef <| selectNewClusterStateLogger (state.RemoveNode node)
                with :? System.Collections.Generic.KeyNotFoundException ->
                    return! Async.Raise (OutOfNodesException "Unable to recover AltMaster failure; cluster is out of nodes.")
            }

            //Throws
            //MessageHandlingException InvalidOperationException => already a master node;; propagate
            try
                //FaultPoint
                //MessageHandlingException InvalidOperationException => already a master node;; propagate
                //FailureException => actor (NodeManager) or node failure => node failure;; remove failed node and retry
                let! newClusterStateLogger = newAltMasterNode <!- fun ch -> AssumeAltMaster(ch, state)
                let newClusterAltMasterNode = { NodeManager = newAltMasterNode; ClusterStateLogger = newClusterStateLogger }

                let updates' = seq { yield! updates; yield ClusterRemoveAltMaster node; yield ClusterAddAltMaster newClusterAltMasterNode }
                let state' = state.RemoveAltMasterNode node
                let state'' = state.AddAltMasterNode newClusterAltMasterNode

                //broadcast new altmasters to cluster
                do! state.Nodes
                    |> Seq.map ReliableActorRef.FromRef
                    |> Broadcast.post (UpdateAltMasters (state''.AltMasters |> List.toArray))
                    |> Broadcast.ignoreFaults Broadcast.allFaults
                    |> Broadcast.exec
                    |> Async.Ignore

                return state'', updates'
            with FailureException _ ->
                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
                let! updates' = removeNode ctx state updates newAltMasterNode
                
                let state' = updateState state updates'
                return! recoveryLoop state' updates'
        }

    async {
        if state.AltMasterNodes |> Seq.tryFind (fun altNode -> altNode = node) |> Option.isSome then
            return! recoveryLoop state updates
        else 
            return state, updates
    }

/// Removes a node and performs any recovery operations required for that node.
//Throws
//SystemCorruptionException => system inconsistency;; SYSTEM FAULT
//OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and removeNode (ctx: BehaviorContext<_>) (state: ClusterState) (updates: seq<ClusterStateUpdate>) (node: ActorRef<NodeManager>) =
    async {
        //Throws
        //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
        //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
        let! state, updates = async {
            try
                //FaultPoint
                //MessageHandlingException InvalidOperationException => already a master node;; inconsistency;; reraise to SystemCorruptionException
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
                return! recoverAltMaster ctx state updates node
            with MessageHandlingException2(:? InvalidOperationException as e) ->
                return! Async.Raise <| SystemCorruptionException(sprintf "System inconsistency occurred while recovering node %O" node, e)
        }

        let resolvedAffectedDefinitions =
            Query.from state.Db.ClusterActiveDefinition
            |> Query.where <@ fun clusterActiveDefinition -> clusterActiveDefinition.NodeManager = node @>
            |> Query.toSeq
            |> Seq.map (fun { ActivationReference = activationRef; Configuration = configuration } -> activationRef, configuration)
            |> Seq.map (function { Definition = LiteralDefinition def; InstanceId = instanceId }, config -> Some(def, instanceId, config) 
                                 | (activationRef, config) -> state.DefinitionRegistry.TryResolve(activationRef.Definition) 
                                                              |> Option.map (fun def -> def, activationRef.InstanceId, config))

        //all affected definitions must be in the definition registry
        assert (resolvedAffectedDefinitions |> Seq.forall Option.isSome) 

        let affectedDefinitions = resolvedAffectedDefinitions |> Seq.choose id |> Seq.toList

        //FaultPoint
        //SystemCorruptionException => unexpected reactivation failure;; SYSTEM FAULT;; propagate
        //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
        return! affectedDefinitions |> List.foldAsync (recoverDefinitionNodeLoss ctx node state) updates
    }

/// The behavior of a ClusterManager in the "proper" state. The behavior transits to the "failed" state when a system fault occurs.
//TODO!!!
//Make sure that in case of alt master node recovery, in case we run out of nodes a warning is triggered instead of a system fault
let rec clusterManagerBehaviorProper (ctx: BehaviorContext<ClusterManager>) 
                                     (state: ClusterState) 
                                     (msg: ClusterManager): Async<Transition<ClusterState, ClusterManager>> =

    /// Returns the current cluster state updated according to the stateUpdates.
    /// Before it returns, it logs this update to the ClusterStateLoggers (in AltMaster nodes).
    //Throws
    //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
    //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
    //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
    let updateState stateUpdates = async {
        let state' = updateState state stateUpdates
        try
            //FaultPoint
            //ClusterStateLogBroadcastException => some updates have failed (node failures);; remove failed nodes and continue
            //ClusterStateLogFailureException => total failure to update (all alt master node failures);; FAULT TOLERANCE COMPROMISE WARNING
            do! logClusterUpdate state <| ClusterUpdate.FromUpdates stateUpdates

            return stay state'
        with ClusterStateLogBroadcastException(_, failedNodes) ->
                let! state'', stateUpdates' = 
                    failedNodes |> List.foldAsync (fun (state, updates) failedNode -> async {
                        //FaultPoint
                        //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
                        //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
                        let! updates' = removeNode ctx state updates failedNode
                        return updateState state updates', Seq.append updates updates'
                    }) (state', stateUpdates)

                return stay <| updateState state'' stateUpdates'
            | ClusterStateLogFailureException msg ->
                //TRIGGER FAULT TOLERANCE COMPROMISE WARNING
                ctx.LogWarning(sprintf "FAULT TOLERANCE COMPROMISED! COMPLETE FAILURE TO REPLICATE STATE. Any failure after this point not tolerated: %s" msg)
                return stay state'
    }

    async {
        match msg with
        | ActivateDefinition(RR ctx reply, activationRecord) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //FaultPoint
                //CompensationFault => Failure in rollback ;; Compensations must be exception safe;; SYSTEM FAULT
                //FinalizationFault => Failure in finalization;; Finalizations must be exception safe;; SYSTEM FAULT
                //InvalidActivationStrategy => invalid argument ;; reversal successfull;; reply exception
                //CyclicDefininitionDependency => invalid configuration ;; reversal successfull;; reply exception
                //MessageHandlingException KeyNotFoundException ;; from NodeManager => definition.Path not found in node;; reply exception
                //MessageHandlingException PartialActivationException ;; from NodeManager => failed to properly recover an activation failure;; SYSTEM FAULT
                //MessageHandlingException ActivationFailureException ;; from NodeManager => failed to activate definition.Path;; reply exception
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesException => unable to recover due to lack of node;; reply exception
                let! stateUpdates = activationLoop ctx state activationRecord

                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                let! transition = updateState stateUpdates

                reply nothing

                return transition
            with InvalidActivationStrategy _ 
                | CyclicDefininitionDependency _
                | OutOfNodesException _ as e ->
                    reply <| Exception e
                    return stay state
                | MessageHandlingException2(:? System.Collections.Generic.KeyNotFoundException as e) ->
                    reply <| Exception e
                    return stay state
                | MessageHandlingException2(ActivationFailureException _ as e) ->
                    reply <| Exception e
                    return stay state
                | CompensationFault _
                | FinalizationFault _ as e ->
                    reply <| Exception(SystemCorruptionException("Unrecoverable system failure.", e))
                    return! triggerSystemFault state
                | MessageHandlingException2(PartialActivationException _ as e) ->
                    reply <| Exception(SystemCorruptionException("Unrecoverable system failure.", e))
                    return! triggerSystemFault state
                | SystemCorruptionException _ as e ->
                    reply <| Exception e
                    return! triggerSystemFault state
                | e ->
                    reply <| Exception(SystemCorruptionException("Unrecoverable system failure.", e))
                    return! triggerSystemFault state

        | DeActivateDefinition(RR ctx reply, activationRef) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                let activationsPerNode =
                    state.Db.ClusterActivation |> ClusterActivation.ResolveActivation activationRef
                    |> Seq.groupBy (fun { NodeManager = nodeManager } -> nodeManager)

                //Throws
                //SystemCorruptionException => a deactivation has thrown an exception
                do! activationsPerNode
                    |> Seq.map (fun (nodeManager, activations) -> async {
                        for activation in activations do
                            //FaultPoint
                            //MessageHandlingException => exception thrown in deactivation;; SYSTEM FAULT;; reraise to SystemCorruptionException
                            //FailureException => node failure;; do not handle;; allow to be detected later
                            try
                                do! ReliableActorRef.FromRef nodeManager <!- fun ch -> DeActivate(ch, activation.ActivationReference, false)
                            with MessageHandlingException2 e ->
                                    return! Async.Raise <| SystemCorruptionException("Unexpected failure occurred.", e)
                                | FailureException _ -> () //do nothing;; allow failure to be detected later
                    })
                    |> Async.Parallel
                    |> Async.Ignore

                reply nothing

                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesException => unable to recover due to lack of node;; SYSTEM FAULT
                return! ClusterDeActivateDefinition activationRef |> Seq.singleton |> updateState
            with SystemCorruptionException _
                | OutOfNodesException _ as e->
                    reply <| Exception e
                    return! triggerSystemFault state
                | e ->
                    reply <| Exception(SystemCorruptionException("Unexpected failure occurred.", e))
                    return! triggerSystemFault state

        | RemoveNode nodeManager ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                let! updates = removeNode ctx state Seq.empty nodeManager

                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                return! updateState updates
            with e ->
                ctx.LogError e

                return! triggerSystemFault state

        | AddNode nodeManager ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            try
                let state' = state.AddNode nodeManager

                //attach the node to the cluster
                //FaultPoint
                //FailureException => node failure ;; do not add anything
                ReliableActorRef.FromRef nodeManager <-- AttachToCluster { Master = state.Master; AltMasters = state.AltMasters }

                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                return! updateState [ClusterAddNode nodeManager]
            with FailureException _ ->
                    return stay state
                | e ->
                    ctx.LogError e
                
                    return! triggerSystemFault state

        | HeartBeat nodeManager ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            return stay (state.UpdateHeartBeat nodeManager)

        | HealthCheck ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                let! updates = 
                    state.DeadNodes 
                    |> Seq.toList 
                    |> List.foldAsync (removeNode ctx state) Seq.empty 

                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                return! updateState updates
            with e ->
                ctx.LogError e

                return! triggerSystemFault state

        | KillCluster ->
            
            try
                //tell all slave nodes to detach themselves
                do! state.Nodes
                    |> Seq.map ReliableActorRef.FromRef
                        //FaultPoint
                        //FailureException => node failure ;; do nothing
                    |> Broadcast.post DetachFromCluster
                        //All failures ignored;; we are killing the cluster anyway
                    |> Broadcast.ignoreFaults Broadcast.allFaults
                    |> Broadcast.exec
                    |> Async.Ignore

                //tell the node manager to cleanup this cluster manager
                state.MasterNode <-- FiniCluster state.ClusterId

                return stay state
            with e ->
                //unexpected error
                ctx.LogError e

                return! triggerSystemFault state
    }

//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and triggerSystemFault (state: ClusterState) =
    async {
        do! state.Nodes
            |> Seq.map ReliableActorRef.FromRef
                //FaultPoint
                //FailureException => node failure ;; do nothing
            |> Broadcast.post TriggerSystemFault
                //All failures ignored;; we are going to failed state anyway
            |> Broadcast.ignoreFaults Broadcast.allFaults
            |> Broadcast.exec
            |> Async.Ignore

        return goto clusterManagerBehaviorSystemFault state
    }

and clusterManagerBehaviorSystemFault (ctx: BehaviorContext<ClusterManager>) (state: ClusterState) (msg: ClusterManager) =
    let reply r = r <| Exception(SystemFailureException "System is in failed state.")
    let warning () = ctx.LogWarning "System is in failed state. No message is processed."
    async {
        match msg with
        | ActivateDefinition(RR ctx r, _) -> reply r
        | DeActivateDefinition(RR ctx r, _) -> reply r
        | RemoveNode _
        | AddNode _
        | HeartBeat _
        | KillCluster //TODO!!! Actually kill the cluster
        | HealthCheck -> warning()
        return stay state
    }

//Throws ;; nothing
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let createClusterManager (state: NodeState) (clusterState: ClusterState) =
    //function should be exception free
    let clusterManager =
        Actor.bind <| FSM.fsmBehavior (goto clusterManagerBehaviorProper clusterState)
        |> Actor.rename "clusterManager"
        |> Actor.publish [UTcp()]
        |> Actor.start

    let managedCluster = {
        ClusterManager = clusterManager
        AltMasters = clusterState.AltMasters
    }

    { state with ManagedClusters = state.ManagedClusters |> Map.add clusterState.ClusterId managedCluster }
    

//Throws ;; nothing
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let createAltMaster (state: NodeState) clusterState =
    //function should be exception free
    let clusterStateLogger = 
        Actor.bind <| Behavior.stateful clusterState clusterStateLoggerBehavior
        |> Actor.rename "clusterStateLogger"
        |> Actor.publish [UTcp()]
        |> Actor.start

    { state with ClusterStateLogger = Some clusterStateLogger }

//Throws
//(By reply) InvalidOperationException => already a master node
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let assumeAltMaster (state: NodeState) reply clusterState =
    async {
        if state.ClusterStateLogger.IsSome then
            reply <| Exception(new InvalidOperationException(sprintf "Node %O is already an AltMaster." state.Address))
            return stay state
        else
            try
                //Should be exception free
                let state' = createAltMaster state clusterState

                reply <| Value state'.ClusterStateLogger.Value.Ref //TODO!!! Make an exception safe reply

                return stay state'
            with e ->
                reply <| Exception e

                return stay state
    }

//Throws ;; nothing
let clearState (ctx: BehaviorContext<_>) (state: NodeState) = async {
    let state' = { 
        state with 
            ActivationsMap = Map.empty 
            ClusterStateLogger = None
            ClusterInfo = None
    }

    try
        if state.ClusterStateLogger.IsSome then
            state.ClusterStateLogger.Value.Stop()

        for activation in state.ActivationsMap |> Map.toSeq |> Seq.map snd do
            do! activation.DeactivateAsync()
    with e -> ctx.LogWarning e
        
    return state'
}

//Throws
//KeyNotFoundException => unknown cluster
let rec finiCluster (state: NodeState) (clusterId: ClusterId) =
    async {
        //Throws
        //KeyNotFoundException => unknown cluster
        let managedCluster = state.ManagedClusters.[clusterId]

        managedCluster.ClusterManager.Stop()

        let state' = { state with ManagedClusters = state.ManagedClusters |> Map.remove clusterId }

        return state'
    }

//Throws ;; nothing
and gotoNodeSystemFault (ctx: BehaviorContext<_>) (state: NodeState) = 
    async {
        let! state' = clearState ctx state

        try
            do! state.EventManager.OnSystemFault()
        with e ->
            //TODO!!! TRIGGER WARNING
            ()

        return goto nodeManagerSystemFault state'
    }

/// The behavior of a NodeManager in the "initialized" state. This is when the node is either not attached to a cluster,
/// or when the node has been notified that the master node of the cluster has been lost.
and nodeManagerInit (ctx: BehaviorContext<NodeManager>) (state: NodeState) (msg: NodeManager) =
    async {
        match msg with
        | InitCluster(RR ctx reply, clusterConfiguration) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //create initial cluster state
                //TODO!!! Make time parameters configurable
                let self = state.NodeManager
                let initClusterState = ClusterState.New(clusterConfiguration.ClusterId, self, TimeSpan.FromSeconds(15.0), state.DefinitionRegistry)

                //add nodes to state
                let clusterState =
                    clusterConfiguration.Nodes |> Seq.fold (fun (clusterState: ClusterState) node -> clusterState.AddNode node) initClusterState

                //select alt masters
                let altMasters = clusterConfiguration.Nodes |> Seq.truncate clusterConfiguration.FailoverFactor

                //TODO!!! TRIGGER WARNING IF alt masters less than failover factor

                //create alt masters
                let! clusterAltMasterNodes =
                    altMasters
                    |> Broadcast.action (fun node -> async {
                            //FaultPoint
                            //InvalidOperationException => already a master node ;; do nothing, ignore this node
                            //FailureException => node failure ;; do nothing, ignore this node
                            let! clusterStateLogger = node <!- fun ch -> AssumeAltMaster(ch, clusterState)
                            return { NodeManager = node; ClusterStateLogger = clusterStateLogger }
                        })
                    //All failures are ignored
                    |> Broadcast.ignoreFaults Broadcast.allFaults
                    |> Broadcast.exec

                //TODO!!! Trigger warning if less alt masters activated than failover factor (if failures occurred)

                //cluster state with alt masters added
                let clusterStateUpdate = 
                    clusterAltMasterNodes |> Seq.map ClusterAddAltMaster
                    |> ClusterUpdate.FromUpdates

                let clusterState' = ClusterUpdate.Update(clusterState, clusterStateUpdate)

                //update alt masters and create cluster state manager
                let! failures = async {
                    try
                        //Throws
                        //ClusterStateLogBroadcastException => some updates have failed (node failures) ;; tell cluster manager of failures
                        //ClusterStateLogFailureException => total failure to update (all alt master node failures) ;; trigger MASSIVE WARNING
                        do! logClusterUpdate clusterState' clusterStateUpdate

                        return []
                    with ClusterStateLogBroadcastException(_, failures) ->
                            return failures
                        | ClusterStateLogFailureException _ ->
                            //TODO!!! TRIGGER MASSIVE WARNING
                            return clusterState'.AltMasterNodes |> Seq.toList
                }

                let totalAltMasterFailure =
                    failures = (Seq.toList clusterState'.AltMasterNodes)

                let clusterState'' =
                    if totalAltMasterFailure then
                        failures |> Seq.fold (fun (clusterState: ClusterState) failedNode -> clusterState.RemoveNode failedNode) clusterState'
                    else clusterState'

                //create cluster manager
                let state' = createClusterManager state clusterState''

                if not totalAltMasterFailure then
                    for failedNode in failures do
                        //In memory communication
                        state'.ManagedClusters.[clusterState.ClusterId].ClusterManager.Ref <-- RemoveNode failedNode

                //reply the addresses of alt master nodes
                if totalAltMasterFailure then
                    reply <| Value Array.empty
                else
                    let clusterState''' =
                        failures |> Seq.fold (fun (clusterState: ClusterState) failedNode -> clusterState.RemoveNode failedNode) clusterState''
                    clusterState'''.AltMasters |> List.toArray |> Value |> reply

                //Throws
                //Exception => SYSTEM FAULT
                do! state.EventManager.OnClusterInit()

                return goto nodeManagerProper state'
            with e ->
                //unexpected exception occurred ;; goto failed state
                reply <| Exception e
                
                return! gotoNodeSystemFault ctx state

        | FiniCluster clusterId ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //Throws
                //KeyNotFoundException => unknown cluster ;; trigger warning
                let! state' = finiCluster state clusterId

                return stay state'
            with e ->
                //unexpected error
                ctx.LogError e
                return! gotoNodeSystemFault ctx state

        | Activate(RR ctx reply, _, _, _) ->
            reply <| Exception(NodeNotInClusterException <| sprintf "Node %O is not part of an active cluster." state.Address)

            return stay state

        | DeActivate(RR ctx reply, _, _) ->
            reply <| Exception(NodeNotInClusterException <| sprintf "Node %O is not part of an active cluster." state.Address)

            return stay state

        | NotifyDependencyLost _ | NotifyDependencyRecovered _ ->
            //In this state this message is invalid
            ctx.LogWarning(sprintf "Unable to process %A in current node state." msg)

            return stay state

        | MasterNodeLoss ->
            //In this state this message is invalid
            ctx.LogWarning(sprintf "Unable to process %A in current node state." msg)

            return stay state

        | AttachToCluster clusterInfo ->
            try
                do! state.EventManager.OnAddToCluster()
            with e -> ctx.LogWarning e

            return goto nodeManagerProper { state with ClusterInfo = Some clusterInfo }

        | DetachFromCluster ->
            //In this state this message is invalid
            ctx.LogWarning(sprintf "Unable to process %A in current node state." msg)

            return stay state

        | AssumeAltMaster(RR ctx reply, clusterState) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            return! assumeAltMaster state reply clusterState

        | UpdateAltMasters _ ->
            //In this state this message is invalid
            ctx.LogWarning(sprintf "Unable to process %A in current node state." msg)

            return stay state

        | TriggerSystemFault ->
            return! gotoNodeSystemFault ctx state
    }

///The behavior of a NodeManager in the "normal" state.
and nodeManagerProper (ctx: BehaviorContext<NodeManager>) (state: NodeState) (msg: NodeManager) =
    async {
        match msg with
        | Activate(RR ctx reply, activationRef, configuration, dependencyActivations) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //Throws
                //KeyNotFoundException => argument error;; allow to fall through;; will be replied
                let definition = state.DefinitionRegistry.Resolve activationRef.Definition
                state.NodeRegistry.RegisterBatch dependencyActivations

                //Throws
                //PartialActivationException => allow to fall through;; will be replied
                //ActivationFailureException => allow to fall through;; will be replied
                let! activation = async {
                    let activateF =
                        match state.ActivationPatterns |> Seq.map (fun p -> p activationRef)
                              |> Seq.tryFind Option.isSome with
                        | Some(Some(activateF, _)) -> activateF
                        | Some None
                        | None -> definition.ActivateAsync

                    try
                        //Throws
                        //CompensationFault => activation failed; recovery of activation failed;; reraise to PartialActivationException
                        //FinalizationFault => activation failed; recovery successfull;; finalization failed;; reraise to PartialActivationException
                        //exn => activation failed; recovery sucessfull; reraise to ActivationFailureException
                        return! RevAsync.ToAsync <| activateF(activationRef.InstanceId, configuration)
                    with CompensationFault e 
                         | FinalizationFault e -> return! Async.Raise <| PartialActivationException((sprintf "Activation of (%O, %d) failed. Recovery of failure failed." activationRef.Definition activationRef.InstanceId), e)
                         | e -> return! Async.Raise <| ActivationFailureException((sprintf "Activation of (%O, %d) failed." activationRef.Definition activationRef.InstanceId), e)
                }
            
                activation.ActivationResults 
                |> Seq.toArray
                |> Value
                |> reply

                return stay { state with ActivationsMap = state.ActivationsMap |> Map.add activationRef activation }
            with e ->
                reply <| Exception e

                return stay state

        | DeActivate(RR ctx reply, activationRef, throwOnNotExisting) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                match state.ActivationsMap.TryFind activationRef with
                | Some activation ->
                    let deactivate =
                        match state.ActivationPatterns |> Seq.map (fun p -> p activationRef)
                                                       |> Seq.tryFind Option.isSome with
                        | Some(Some(_, deactive)) -> deactive
                        | Some None
                        | None -> activation.DeactivateAsync()

                    //Throws
                    //e => Failed to deactivated;; allow to fail;; exception will be replied
                    do! deactivate
                    //do! activation.DeactivateAsync()

                    reply nothing

                    return stay { state with ActivationsMap = state.ActivationsMap |> Map.remove activationRef }
                | None ->
                    if throwOnNotExisting then
                        reply <| Exception (new System.Collections.Generic.KeyNotFoundException(sprintf "No activation found by activation reference: %A" activationRef))
                    else reply nothing

                    return stay state
            with e ->
                reply <| Exception e
                return stay state

        | NotifyDependencyLost(dependant, dependency) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            try
                //TODO!!! Also make sure that the dependant is indeed activated on this node

                //Throws
                //KeyNotFoundException => dependant does not exist in this node
                let dependantDefinition = state.DefinitionRegistry.Resolve dependant

                do! dependantDefinition.OnDependencyLoss dependency

                return stay state
            with :? System.Collections.Generic.KeyNotFoundException as e ->
                    ctx.LogWarning e

                    return stay state
                | e ->
                    //unexpected exception
                    ctx.LogError e

                    return! gotoNodeSystemFault ctx state

        | NotifyDependencyRecovered(dependant, dependency) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            try
                //TODO!!! Also make sure that the dependant is indeed activated on this node

                //Throws
                //KeyNotFoundException => dependant does not exist in this node
                let dependantDefinition = state.DefinitionRegistry.Resolve dependant

                do! dependantDefinition.OnDependencyLossRecovery dependency

                return stay state
            with :? System.Collections.Generic.KeyNotFoundException as e ->
                    ctx.LogWarning e
                    return stay state
                | e ->
                    //unexpected exception
                    ctx.LogError e
                    return! gotoNodeSystemFault ctx state

        | AssumeAltMaster(RR ctx reply, clusterState) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            return! assumeAltMaster state reply clusterState

        | AttachToCluster _ -> 
            //In this state this message is invalid
            ctx.LogWarning(sprintf "Unable to process %A in current node state." msg)

            return stay state

        | DetachFromCluster ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            let! state' = clearState ctx state

            try
                do! state'.EventManager.OnRemoveFromCluster()
            with e -> ctx.LogWarning e

            return goto nodeManagerInit state'

        | UpdateAltMasters newAltMasters ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //Throws
                //ArgumentException => state.ClusterInfo = None ;; SYSTEM FAULT
                return stay { state with ClusterInfo = Some { state.ClusterInfo.Value with AltMasters = newAltMasters |> Array.toList } }
            with e ->
                ctx.LogError e
                return! gotoNodeSystemFault ctx state

        | MasterNodeLoss when state.IsAltMaster ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            let altMasterIndex = state.AltMasterIndex
            if altMasterIndex = 0 then
                //If the current node is the first in the list of alt masters
                //then trigger master node recovery
                return! recoverMasterNode ctx state
            else
                //If the current node is an alternative master
                //wait for attachment to new master (in nodeManagerInit state)
                //for a given time. Master node recovery is given a specific
                //timeout (say 30 seconds). Each alt master node waits for that
                //time, times the number of alt master nodes with higher priority.
                //E.g. the second alt master waits for 30 secs, the third for 60 secs and so forth
                //If an alt master times out, then it starts master node recovery.
                //This way, someone is guarnteed to start master node recovery.
                //The timeout must be sufficiently large to prevent 2 master node recoveries
                //occurring concurrently.
                return goto nodeManagerInit state 
                       |> onTimeout (30000 * (altMasterIndex + 1)) (recoverMasterNode ctx state) //TODO!!! Make the timeout configurable

        | MasterNodeLoss -> 
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            //Each node that is a master node
            //waits for attachment to new master node (in nodeManagerInit state)
            //for twice the time it takes for master node recovery to occur
            //(master node dies and during recovery all alt masters die except the last one).
            //If the time runs out, then each node goes to failed state.
            return goto nodeManagerInit state //TODO!!! Make the timeout configurable (same as above)
                   |> onTimeout (30000 * state.ClusterStateLoggers.Length * 2) (gotoNodeSystemFault ctx state)

        | TriggerSystemFault -> return! gotoNodeSystemFault ctx state

        | InitCluster(RR ctx reply, _) ->
            reply <| Exception (ClusterInitializedException "Cluster has already been initialized.")

            return stay state

        | FiniCluster clusterId ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //Throws
                //KeyNotFoundException => unknown cluster ;; trigger warning
                let! state' = finiCluster state clusterId

                return stay state'
            with e ->
                //unexpected error
                ctx.LogError e

                return! gotoNodeSystemFault ctx state
    }

//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and recoverMasterNode (ctx: BehaviorContext<_>) (state: NodeState) = 
    //Throws
    //InvalidOperationException => already a master node ;; SYSTEM FAULT
    let rec selectNewClusterStateLoggerLoop clusterState clusterState'' nodeFailuresDetected = async {
        try
            //select candidate
            //Throws
            //KeyNotFoundException => no nodes available ;; failover capacity reduced ;; TRIGGER MASSIVE WARNING
            let newAltMasterNode = selectNewClusterStateLogger clusterState''

            try
                //FaultPoint
                //InvalidOperationException => already a master node ;; SYSTEM FAULT
                //FailureException => node failure ;; retry and remove node after recovery
                let! newClusterStateLogger = newAltMasterNode <!- fun ch -> AssumeAltMaster(ch, clusterState)

                //cluster state of clustermanager
                //the old altmaster is replaced by the new one
                //but the dead master node data must still be there
                //so that the clustermanager can perfrom the recovery properly
                let newClusterAltMasterNode = { NodeManager = newAltMasterNode; ClusterStateLogger = newClusterStateLogger }
                let newClusterState = clusterState.RemoveAltMasterNode state.NodeManager
                let newClusterState = newClusterState.AddAltMasterNode newClusterAltMasterNode

                try
                    //update clusterstateloggers
                    //FaultPoint
                    //ClusterStateLogBroadcastException => some updates have failed (node failures) ;; remove failed nodes after recovery
                    //ClusterStateLogFailureException => total failure to update (all alt master node failures) ;; continue but TRIGGER MASSIVE WARNING
                    do! logClusterUpdate newClusterState <| ClusterUpdate.FromUpdates [ClusterRemoveAltMaster state.NodeManager; ClusterAddAltMaster newClusterAltMasterNode]

                    return newClusterState, nodeFailuresDetected
                with ClusterStateLogBroadcastException(_, failedNodes) ->
                        return newClusterState, failedNodes@nodeFailuresDetected
                    | ClusterStateLogFailureException msg ->
                        ctx.LogWarning(sprintf "FAULT TOLERANCE COMPROMISED DURING MASTER NODE RECOVERY! COMPLETE FAILURE TO REPLICATE STATE. Any failure after this point not tolerated: %s" msg)

                        return newClusterState, (newClusterState.AltMasterNodes |> Seq.toList)@nodeFailuresDetected
            with FailureException _ ->
                return! selectNewClusterStateLoggerLoop clusterState (clusterState''.RemoveNode newAltMasterNode) (newAltMasterNode::nodeFailuresDetected)
        with :? System.Collections.Generic.KeyNotFoundException ->
            ctx.LogWarning "UNABLE TO SELECT NEW ALTMASTER NODE. NO CANDIDATE NODES AVAILABLE. FAILOVER FACTOR REDUCED."

            return clusterState, nodeFailuresDetected
    }

    async {
        try
            //throws if this is not alt master node ;; inconsistent state
            let lostMasterNode = state.ClusterInfo.Value.MasterNode

            //get cluster state
            let! clusterState = state.ClusterStateLogger.Value.Ref <!- GetClusterState

            //cluster state with master node removed 
            let clusterState' = clusterState.RemoveNode lostMasterNode
            //and without current node
            let clusterState'' = clusterState'.RemoveNode state.NodeManager

            //create new cluster state logger
            //Throws
            //InvalidOperationException => already a master node ;; SYSTEM FAULT
            let! newClusterState, nodeFailuresDetected = selectNewClusterStateLoggerLoop clusterState clusterState'' []
            let newClusterState = { newClusterState with MasterNode = state.NodeManager }

            //create cluster manager from logger data
            let state' = createClusterManager state newClusterState
            let clusterManager = state'.ManagedClusters.[newClusterState.ClusterId].ClusterManager.Ref
            
            //notify cluster
            let! nodeFailuresDetected' = async {
                try
                    //FaultPoint
                    //BroadcastFailureException => total failure in broadcast ;; TRIGGER MASSIVE WARNING
                    //BroadcastPartialFailureException => some posts have failed ;; remove nodes after recovery
                    do! clusterState''.Nodes
                        |> Seq.map ReliableActorRef.FromRef
                            //FaultPoint
                            //FailureException => node failure ;; remove node after recovery
                        |> Broadcast.post (AttachToCluster { Master = state.Address; AltMasters = newClusterState.AltMasters })
                        |> Broadcast.exec
                        |> Async.Ignore

                    return nodeFailuresDetected
                with BroadcastFailureException _ ->
                        //TRIGGER MASSIVE WARNING ;; do not remove nodes, let the cluster manager handle this one
                        ctx.LogWarning "FAILED TO ATTACH SLAVE NODES TO NEW MASTER NODE. ALL CLUSTER NODES FAILED."
                        return nodeFailuresDetected
                    | BroadcastPartialFailureException(_, failedNodes) ->
                        return (failedNodes |> List.map (fun (n, _) -> n :?> ActorRef<NodeManager>))@nodeFailuresDetected
            }

            //trigger node recovery
            clusterManager <-- RemoveNode lostMasterNode
            //trigger recovery of detected failed nodes
            for failedNode in nodeFailuresDetected' do clusterManager <-- RemoveNode failedNode

            return stay { state' with ClusterInfo = None }
        with e ->
            //unexpected exception at this point
            ctx.LogError e
            return! gotoNodeSystemFault ctx state
    }

and nodeManagerSystemFault (ctx: BehaviorContext<NodeManager>) (state: NodeState) (msg: NodeManager) =
    let reply r = r <| Exception(SystemFailureException "System is in failed state.")
    let warning () = ctx.LogWarning "System is in failed state. No message is processed."
    async {
        match msg with
        | Activate(RR ctx r, _, _, _) -> reply r
        | DeActivate(RR ctx r, _, _) -> reply r
        | AssumeAltMaster(RR ctx r, _) -> reply r
        | InitCluster(RR ctx r, _) -> reply r
        | FiniCluster _ //TODO!!! Actually finalize the cluster
        | AttachToCluster _
        | DetachFromCluster
        | UpdateAltMasters _
        | MasterNodeLoss
        | NotifyDependencyLost _
        | NotifyDependencyRecovered _
        | TriggerSystemFault -> warning()

        return stay state
    }

let createNodeManager (address: Address) 
                      (definitionRegistry: DefinitionRegistry) 
                      (eventManager: NodeEventManager)
                      (activationPatterns: ActivationPattern list) = 
    let state = NodeState.New(address, NodeRegistry.Registry, definitionRegistry, eventManager, activationPatterns)
    let nodeManager =
        Actor.bind <| FSM.fsmBehavior (goto nodeManagerInit state)
        |> Actor.rename "nodeManager"
        |> Actor.publish [UTcp()]
        |> Actor.start

    eventManager.OnInit()

    nodeManager

