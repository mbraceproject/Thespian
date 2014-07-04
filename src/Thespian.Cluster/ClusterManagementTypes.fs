[<AutoOpen>]
module Nessos.Thespian.Cluster.Common

open System
open Nessos.Thespian
open Nessos.Thespian.Utils
open Nessos.Thespian.AsyncExtensions
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Remote.TcpProtocol.Unidirectional
open Nessos.Thespian.ImemDb
open Nessos.Thespian.Reversible
open Nessos.Thespian.Cluster


exception NodeNotInClusterException of string
exception ClusterInitializedException of string

exception ActivationResolutionException of string
exception DefinitionResolutionException of string
exception CyclicDefininitionDependency of string
exception InvalidActivationStrategy of string

exception ActivationFailureException of string * exn
exception PartialActivationException of string * exn

exception OutOfNodesException of string

exception SystemCorruptionException of string * exn
exception SystemFailureException of string

module Default =

    let actorEventHandlerProtocol (namePrefix: string) (logEntry: Log) =
        let logLevel, logSource, payload = logEntry
//        let logLevel' = match logLevel with Nessos.Thespian.LogLevel.Info -> Info
//                                            | Nessos.Thespian.LogLevel.Warning -> Warning
//                                            | Nessos.Thespian.LogLevel.Error -> Error
        let logSourceStr = match logSource with 
                           | Actor(name, uuid) -> 
                                let name' = if name = String.Empty then "*" else name
                                let uuid' = if name = String.Empty then uuid.ToString() else "*"
                                sprintf "%s/%s" uuid' name'
                           | Protocol name -> "protocol: " + name

        let entryMsg = sprintf "%s: %s %A" namePrefix logSourceStr payload
        Log.logNow logLevel entryMsg

    let actorEventHandlerNoProtocol (onActorFailure: exn -> unit) (namePrefix: string) (logEntry: Log) =
        let logLevel, logSource, payload = logEntry
        match logSource with
        | Protocol _ -> ()
        | _ ->
//            let logLevel' = match logLevel with Nessos.Thespian.LogLevel.Info -> Info
//                                                | Nessos.Thespian.LogLevel.Warning -> Warning
//                                                | Nessos.Thespian.LogLevel.Error -> Error
            let logSourceStr = match logSource with 
                               | Actor(name, uuid) -> 
                                    let name' = if name = String.Empty then "*" else name
                                    let uuid' = if name = String.Empty then uuid.ToString() else "*"
                                    sprintf "%s/%s" uuid' name'
                               | _ -> "Invalid log message. If this appears in the log, this is a bug."

            let entryMsg = sprintf "%s: %s %A" namePrefix logSourceStr payload
            Log.logNow logLevel entryMsg

        match logLevel, payload with
        | Nessos.Thespian.LogLevel.Error, (:? ActorFailedException as e) -> onActorFailure e
        | _ -> ()

    let actorEventHandler = actorEventHandlerNoProtocol

    let fatalActorFailure e = 
//        let logger = IoC.Resolve<ILogger>()
//        logger.LogError e "FATAL ERROR. UNABLE TO CONTINUE."
        System.Threading.Thread.Sleep 4000
        System.Diagnostics.Process.GetCurrentProcess().Kill()

type Conf() =
    static member Spec<'T>(name: string, ?defVal: 'T) =
        name, typeof<'T>, defVal |> Option.map box

    static member Val<'T>(name: string) (value: 'T) = name, box value

type ActivationConfiguration = {
    Specification: Map<string, Type>
    Values: Map<string, obj>
} with
    member conf.Get<'T>(key: string) =
        conf.Values.[key] :?> 'T

    ///Override current configuration with the one given in the parameter.
    member conf.Override(conf': ActivationConfiguration) = ActivationConfiguration.Override(conf, conf')

    static member Empty = {
        Specification = Map.empty
        Values = Map.empty
    }

    static member Specify(specification: (string * Type * obj option) list) = {
        Specification = specification |> Seq.map (fun (k, t, _) -> k, t) |> Map.ofSeq
        Values = specification |> Seq.choose (function k, _, Some v -> Some(k, v) | _ -> None) |> Map.ofSeq
    }

    static member FromValues(values: (string * obj) list) = {
        Specification = values |> Seq.map (fun (k, v) -> k, v.GetType()) |> Map.ofSeq
        Values = values |> Map.ofList
    }

    static member Override(baseConfig: ActivationConfiguration, overridingConfig: ActivationConfiguration) =
        if overridingConfig.Values 
            |> Map.toSeq
            |> Seq.forall (fun (key, value) -> 
                match baseConfig.Specification.TryFind key with
                | Some keyType -> value.GetType().IsAssignableFrom(keyType)
                | None -> true
            ) then
           
            let values' = overridingConfig.Values |> Map.toSeq |> Seq.fold (fun values (key, value) -> Map.add key value values) baseConfig.Values
            let specification' = 
                overridingConfig.Specification 
                |> Map.toSeq 
                |> Seq.fold (fun specs (key, spec) -> match specs |> Map.tryFind key with None -> Map.add key spec specs | _ -> specs) baseConfig.Specification

            {
                Values = values'
                Specification = specification'
            }
        else invalidArg "overridingConfig" "Configuration specification and overriding values mismatch"


and IActivationStrategy =
    abstract GetActivationNodesAsync: ClusterState * int * Definition -> Async<ActorRef<NodeManager> list>

and Collocated() =
    interface IActivationStrategy with
        member __.GetActivationNodesAsync(_, _, _) = async { return! Async.Raise <| new InvalidOperationException("Not supported in Collocated IActivationStrategy.") }

and NodeFaultReActivation(failedNode: ActorRef<NodeManager>) =
    interface IActivationStrategy with
        member __.GetActivationNodesAsync(clusterState: ClusterState, instanceId: int, definition: Definition) = async {
            let nonEmptyNodes =
                let existingDefs =
                    Query.from clusterState.Db.ClusterActiveDefinition
                    |> Query.where <@ fun cad -> cad.ActivationReference.Definition = definition.Path
                                                 && cad.ActivationReference.InstanceId = instanceId @>
                    |> Query.toSeq
                    |> Seq.map (fun cad -> cad.NodeManager)
                    |> Set.ofSeq

                Query.from clusterState.Db.ClusterNode
                |> Query.where <@ fun cn -> not (existingDefs |> Set.contains cn.NodeManager) @>
                |> Query.toSeq
                |> Seq.map (fun cn -> cn.NodeManager)
                |> Seq.distinct

            let emptyNodes =
                Query.from clusterState.Db.ClusterNode
                |> Query.leftOuterJoin clusterState.Db.ClusterActiveDefinition <@ fun (cn, cad) -> cn.NodeManager = cad.NodeManager @>
                |> Query.where <@ fun (_, cad) -> cad.IsNone @>
                |> Query.toSeq
                |> Seq.map (fun (cn, _) -> cn.NodeManager)
                |> Seq.distinct

            return 
                Seq.append nonEmptyNodes emptyNodes
                |> Seq.filter (fun nodeManager -> nodeManager <> failedNode)
                |> Seq.truncate 1
                |> Seq.toList
        }

and ActivationReference = {
    Definition: DefinitionPath
    InstanceId: int
} with
    static member DefaultInstanceId = 0

    static member FromPath path = { Definition = path; InstanceId = ActivationReference.DefaultInstanceId }

    override ar.ToString() = sprintf "%O:%d" ar.Definition ar.InstanceId

//TODO!!! Examine the equality semantics of this
and ActivationRecord = {
    Definition: DefinitionPath
    Instance: int option
    Configuration: ActivationConfiguration
    ActivationStrategy: IActivationStrategy
} with
    member ar.IsCollocated =
        match ar.ActivationStrategy with
        | :? Collocated -> true
        | _ -> false

    static member ToActivationReference(activationRecord: ActivationRecord) =
        { Definition = activationRecord.Definition; InstanceId = activationRecord.Instance |> Option.fold (fun _ id -> id) ActivationReference.DefaultInstanceId }

and NodeLossAction = 
    DoNothingOnNodeLoss 
    | ReActivateOnNodeLoss
    | ReActivateOnNodeLossSpecific of IActivationStrategy

and [<AbstractClass>] Definition(parent: DefinitionPath) as self =
    let formatLogMsg msg = sprintf "def::/%O :: %s" self.Path msg

    abstract Name: string
    abstract Configuration: ActivationConfiguration
    default __.Configuration = ActivationConfiguration.Empty
    ///The definition's dependencies under the default configuration
    abstract Dependencies: ActivationRecord list
    ///Get the definition's dependencies by overriding the default configuration
    abstract GetDependencies: ActivationConfiguration -> ActivationRecord list
    default def.GetDependencies _ = def.Dependencies
    abstract ActivateAsync: int * ActivationConfiguration -> RevAsync<Activation>

    member def.Parent = parent
    abstract Path: DefinitionPath
    default def.Path = parent/def.Name

    abstract Logger : ILogger
    default def.Logger = Logger.DefaultLogger
//    abstract LogEntry: LogEntry -> unit
//    default def.LogEntry entry =
//        let (SystemLog(msg, level, time)) = entry
//        Log.entry <| SystemLog(formatLogMsg msg, level, time)
//
//    abstract LogInfo: string -> unit
//    default def.LogInfo msg = Log.info (formatLogMsg msg)
//
//    abstract LogError: exn -> string -> unit
//    default def.LogError e msg = Log.error e (formatLogMsg msg)
//
//    abstract LogWithException: LogLevel -> exn -> string -> unit
//    default def.LogWithException level e msg = Log.withException level e (formatLogMsg msg)
//    
//    abstract LogMsg: LogLevel -> string -> unit
//    default def.LogMsg level msg = Log.msg level (formatLogMsg msg)
    member __.LogInfo msg = __.Logger.Log (msg, Info, DateTime.Now)
    member __.LogWithException lvl e msg = __.Logger.Log(sprintf "%s\n    Error=%O" msg e, lvl, DateTime.Now)
    member __.LogError e msg = __.LogWithException Error e msg
    member __.LogMsg lvl msg = __.Logger.Log(msg, lvl, DateTime.Now)

    abstract OnActivated: Activation -> RevAsync<unit>
    default __.OnActivated _ = revasync.Zero()

    abstract OnDeactivate: Activation -> Async<unit>
    default __.OnDeactivate _ = async.Zero()

    abstract OnDeactivated: ActivationReference -> Async<unit>
    default __.OnDeactivated _ = async.Zero()

    abstract OnNodeLossAction: ActorRef<NodeManager> -> NodeLossAction
    default __.OnNodeLossAction _ = DoNothingOnNodeLoss

    abstract OnNodeLoss: ActorRef<NodeManager> * ActivationReference -> Async<unit>
    default __.OnNodeLoss(_, _) = async.Zero()

    abstract OnRecovered: ActivationReference -> Async<unit>
    default __.OnRecovered _ = async.Zero()

    abstract OnDependencyLoss: Activation * ActivationReference * ActorRef<NodeManager> -> Async<unit>
    default __.OnDependencyLoss(_, _, _) = async.Zero()

    abstract OnDependencyLossRecovery: Activation * ActivationReference -> Async<unit>
    default __.OnDependencyLossRecovery(_, _) = async.Zero()

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

    member a.TryResolve<'T>(activationRef: ActivationReference) =
        a.ActorActivationResults
        |> Seq.tryFind (fun ar -> ar.ActivationReference = activationRef)
        |> Option.bind (fun ar -> ar.ActorRef)
        |> Option.map (fun actorRef -> actorRef :?> ActorRef<'T>)

    member a.Resolve<'T>(activationRef: ActivationReference) = 
        match a.TryResolve<'T>(activationRef) with
        | Some r -> r
        | None -> raise <| new System.Collections.Generic.KeyNotFoundException("Activation reference not found in activation.")

    member a.IsActivated(activationRef: ActivationReference) =
        a.DefinitionActivationResults
        |> Seq.tryFind (fun ar -> ar.ActivationReference = activationRef)

    abstract ActorActivationResults: seq<ClusterActivation>
    abstract DefinitionActivationResults: seq<ClusterActiveDefinition>

    abstract DeactivateAsync: unit -> Async<unit>
    abstract Deactivate: unit -> unit
    override a.Deactivate() = Async.RunSynchronously <| a.DeactivateAsync()

    interface IDisposable with
        member a.Dispose() = a.Deactivate()

and ClusterActivationsDb = {
    ClusterActivation: Table<ClusterActivation>
    ClusterActiveDefinition: Table<ClusterActiveDefinition>
} with
    static member Create(clusterActivations: #seq<ClusterActivation>, clusterActiveDefinitions: #seq<ClusterActiveDefinition>) = 
        {
            ClusterActivation = clusterActivations |> Table.insertValues (Table.create <@ fun clusterActivation -> clusterActivation.Key @>)
            ClusterActiveDefinition = clusterActiveDefinitions |> Table.insertValues (Table.create <@ fun clusterGroupActivation -> clusterGroupActivation.Key @>)
        }

    static member Create(clusterActivations: Table<ClusterActivation>, clusterActiveDefinitions: Table<ClusterActiveDefinition>) = {
        ClusterActivation = clusterActivations
        ClusterActiveDefinition = clusterActiveDefinitions
    }

    static member Empty = ClusterActivationsDb.Create(Seq.empty, Seq.empty)

    member db.RegisterBatch(activations: #seq<ClusterActivation>, definitions: #seq<ClusterActiveDefinition>) = { 
        ClusterActivation = ClusterActivation.RegisterBatch activations db.ClusterActivation 
        ClusterActiveDefinition = ClusterActiveDefinition.RegisterBatch definitions db.ClusterActiveDefinition
    }

    member db.RegisterBatchOverriding(activations: #seq<ClusterActivation>, definitions: #seq<ClusterActiveDefinition>) = {
        ClusterActivation = ClusterActivation.RegisterBatchOverriding activations db.ClusterActivation 
        ClusterActiveDefinition = Table.insertValues db.ClusterActiveDefinition definitions
    }

    member db.Register(activation: ClusterActivation, definition: ClusterActiveDefinition) =
        db.RegisterBatch(Seq.singleton activation, Seq.singleton definition)

    member db.RegisterActivation(activation: ClusterActivation) =
        db.RegisterBatch(Seq.singleton activation, Seq.empty)

    member db.RegisterDefinition(registration: ClusterActiveDefinition) = 
        db.RegisterBatch(Seq.empty, Seq.singleton registration)

    member db.UnRegister(node: ActorRef<NodeManager>, activationRef: ActivationReference) = { 
        ClusterActivation = ClusterActivation.UnRegister node activationRef db.ClusterActivation 
        ClusterActiveDefinition = ClusterActiveDefinition.UnRegister node activationRef db.ClusterActiveDefinition
    }

    member db.UnRegister(activationRef: ActivationReference) = {
        ClusterActivation = ClusterActivation.UnRegisterActivationRef activationRef db.ClusterActivation
        ClusterActiveDefinition = ClusterActiveDefinition.UnRegisterActivationRef activationRef db.ClusterActiveDefinition
    }

    member db.UnRegisterDescendants(activationRef: ActivationReference) = {
        ClusterActivation = ClusterActivation.UnRegisterActivationRefDescendants activationRef db.ClusterActivation
        ClusterActiveDefinition = ClusterActiveDefinition.UnRegisterActivationRefDescendants activationRef db.ClusterActiveDefinition
    }

    member db.UnRegisterDefinition(node: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Database.remove <@ fun db -> db.ClusterActiveDefinition @> (ClusterActiveDefinition.MakeKey(node, activationRef)) db

    member db.UnRegisterActivation(node: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Database.remove <@ fun db -> db.ClusterActivation @> (ClusterActiveDefinition.MakeKey(node, activationRef)) db

    member db.ResolveActivations(activationRef: ActivationReference) =
        ClusterActivation.ResolveActivation activationRef db.ClusterActivation

    member db.ResolveActivation(activationRef: ActivationReference, node: ActorRef<NodeManager>) =
        ClusterActivation.ResolveNodeActivation activationRef node db.ClusterActivation

    member db.ResolveActiveDefinitions(activationRef: ActivationReference) =
        ClusterActiveDefinition.ResolveActiveDefinitions activationRef db.ClusterActiveDefinition

    member db.IsActivated(activationRef: ActivationReference) =
        Query.from db.ClusterActiveDefinition
        |> Query.where <@ fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference = activationRef @>
        |> Query.toSeq
        |> Seq.isEmpty
        |> not

    member db.IsActivated(activationRef: ActivationReference, node: ActorRef<NodeManager>) =
        Query.from db.ClusterActiveDefinition
        |> Query.where <@ fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference = activationRef 
                                                         && clusterActiveDefinition.NodeManager = node @>
        |> Query.toSeq
        |> Seq.isEmpty
        |> not


and NodeRegistry private () =
    static let registry = Atom.atom ClusterActivationsDb.Empty

    static let mutable instance = new NodeRegistry()

    let mutable currentNode = ActorRef.empty() : ActorRef<NodeManager>

    member private __.CurrentNode with get() = currentNode
                                   and set(currentNode': ActorRef<NodeManager>) = currentNode <- currentNode'

    static member Init(currentNode: ActorRef<NodeManager>) = instance.CurrentNode <- currentNode        

    static member internal Registry = instance

    member __.RegisterBatch(activations: #seq<ClusterActivation>, definitions: #seq<ClusterActiveDefinition>) =
        Atom.swap registry (fun db -> db.RegisterBatch(activations, definitions))

    member __.RegisterBatchOverriding(activations: #seq<ClusterActivation>, definitions: #seq<ClusterActiveDefinition>) =
        Atom.swap registry (fun db -> db.RegisterBatchOverriding(activations, definitions))

    member __.RegisterDefinition(registration: ClusterActiveDefinition) =
        Atom.swap registry (fun db -> db.RegisterDefinition registration)

    member reg.Register(activation: ClusterActivation, definition: ClusterActiveDefinition) =
        reg.RegisterBatch(Seq.singleton activation, Seq.singleton definition)

    member __.UnRegister(node: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Atom.swap registry (fun db -> db.UnRegister(node, activationRef))

    member __.UnRegister(activationRef: ActivationReference) =
        Atom.swap registry (fun db -> db.UnRegister(activationRef))

    member __.UnRegisterDescendants(activationRef: ActivationReference) =
        Atom.swap registry (fun db -> db.UnRegisterDescendants(activationRef))

    member reg.UnRegisterLocal(activatinoRef: ActivationReference) = reg.UnRegister(currentNode, activatinoRef)

    member __.UnRegisterActivation(node: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Atom.swap registry (fun db -> db.UnRegisterActivation(node, activationRef))
    member __.UnRegisterDefinition(node: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Atom.swap registry (fun db -> db.UnRegisterDefinition(node, activationRef))

    member __.Locals = 
        Query.from registry.Value.ClusterActivation
        |> Query.where <@ fun ca -> ca.NodeManager = currentNode @>
        |> Query.toSeq

    member __.All =
        Query.from registry.Value.ClusterActivation
        |> Query.toSeq

    member __.Clear() = Atom.swap registry (fun _ -> ClusterActivationsDb.Empty)

    member __.IsActivated(activationRef: ActivationReference) = registry.Value.IsActivated(activationRef)

    member __.IsActivatedLocally(activationRef: ActivationReference) =
        Query.from registry.Value.ClusterActiveDefinition
        |> Query.where <@ fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference = activationRef 
                                                         && clusterActiveDefinition.NodeManager = currentNode @>
        |> Query.toSeq
        |> Seq.isEmpty
        |> not

    member __.Resolve<'T>(activationRef: ActivationReference) = ClusterActivation.ResolveExact<'T> activationRef registry.Value.ClusterActivation

    member __.ResolveLocalActorActivation(activationRef: ActivationReference) =
        Query.from registry.Value.ClusterActivation
        |> Query.where <@ fun ca -> ca.ActivationReference = activationRef && ca.NodeManager = currentNode @>
        |> Query.toSeq
        |> Seq.tryHead

    ///Returns a sequence of cluster activations of the given activation reference that are available in the node registry.
    member __.ResolveActivations(activationRef: ActivationReference) = registry.Value.ResolveActivations(activationRef)

    member __.ResolveActivation(activationRef: ActivationReference, node: ActorRef<NodeManager>) = registry.Value.ResolveActivation(activationRef, node)

    member __.ResolveActiveDefinitions(activationRef: ActivationReference) = registry.Value.ResolveActiveDefinitions(activationRef)

    member __.ResolveLocalActivationData(activationRef: ActivationReference) =
        let db = registry.Value
        Query.from db.ClusterActivation
        |> Query.where <@ fun clusterActivation -> clusterActivation.ActivationReference = activationRef @>
        |> Query.toSeq
        ,
        Query.from db.ClusterActiveDefinition
        |> Query.where <@ fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference = activationRef @>
        |> Query.toSeq
    
    member __.TryResolveLocal(activationRef: ActivationReference) =
        Query.from registry.Value.ClusterActivation
        |> Query.where <@ fun ca -> ca.ActivationReference = activationRef && ca.NodeManager = currentNode @>
        |> Query.toSeq
        |> Seq.map (fun ca -> ca.ActorRef)
        |> Seq.choose id
        |> Seq.tryHead

    member reg.ResolveLocal(activationRef: ActivationReference) =
        let result = reg.TryResolveLocal(activationRef)
        if result.IsNone then raise  (ActivationResolutionException <| sprintf "%O:%A not found in local node." activationRef.Definition activationRef.InstanceId)
        else result.Value

    member reg.TryResolveLocal<'T>(activationRef: ActivationReference) =
        reg.TryResolveLocal activationRef
        |> Option.map (fun actorRef -> actorRef :?> ActorRef<'T>)

    //Throws
    //ActivationResolutionException => activationRef not found
    //TypeCastException => invalid message type expected
    member reg.ResolveLocal<'T>(activationRef: ActivationReference) = 
        reg.ResolveLocal(activationRef) :?> ActorRef<'T>

and [<AbstractClass>] BaseActorDefinition<'T>(parent: DefinitionPath) =
    inherit Definition(parent)

    let actorPathName (ap: DefinitionPath) (instanceId: int) = 
        sprintf "%s.%d" (ap.ToString().Replace("/", ".").Substring(1)) instanceId

    abstract Actor: ActivationConfiguration * int -> Async<Actor<'T>>
    abstract GetActorRef: Actor<'T> -> ActorRef
    default def.GetActorRef(actor: Actor<'T>) = actor.Ref :> ActorRef
    abstract PublishProtocols: IProtocolConfiguration list
    override __.PublishProtocols = [new Unidirectional.UTcp()]

    abstract OnActorFailure: int * ActivationConfiguration -> (exn -> unit)
    default __.OnActorFailure(_, _) = ignore

    abstract OnActorEvent: int * ActivationConfiguration -> (Log -> unit)
    default def.OnActorEvent(instanceId, configuration) =
        Default.actorEventHandler (def.OnActorFailure(instanceId, configuration)) String.Empty

    member def.IsLocal = def.PublishProtocols.IsEmpty

    override def.ActivateAsync(instanceId: int, configuration: ActivationConfiguration) = 
        RevAsync.FromAsync(
            async {
                let! actor' = def.Actor(configuration, instanceId)

                let actor'' = actor' |> Actor.rename (actorPathName def.Path instanceId)
                                     |> Actor.subscribeLog (def.OnActorEvent(instanceId, configuration))

                let actor = if def.IsLocal then actor'' else actor'' |> Actor.publish def.PublishProtocols
                            |> Actor.start

                let activationRef = { Definition = def.Path; InstanceId = instanceId }
                do NodeRegistry.Registry.Register( 
                    { 
                        NodeManager = Cluster.NodeManager
                        ActivationReference = activationRef
                        ActorRef = if def.IsLocal then None else Some(def.GetActorRef(actor))
                        Configuration = configuration 
                    },
                    {
                        NodeManager = Cluster.NodeManager
                        ActivationReference = activationRef
                        Configuration = configuration
                    }
                )

                return new ActorActivation<'T>(instanceId, actor, def, configuration) :> Activation
            },
            (fun actorActivation -> actorActivation.DeactivateAsync()) ///TODO!!! Handle exceptions
        )

and [<AbstractClass>] ActorDefinition<'T>(parent: DefinitionPath) =
    inherit BaseActorDefinition<'T>(parent)
    
    abstract Behavior: ActivationConfiguration * int -> Async<Actor<'T> -> Async<unit>>

    override def.Actor(configuration: ActivationConfiguration, instanceId: int) = async {
        let! behavior = def.Behavior(configuration, instanceId)

        return Actor.bind behavior
    }

and ActorActivation<'T>(instanceId: int, actor: Actor<'T>, definition: BaseActorDefinition<'T>, configuration: ActivationConfiguration) =
    inherit Activation(instanceId, definition)

    let disposable = actor :> IDisposable

    override __.ActorActivationResults = Seq.singleton {
        NodeManager = Cluster.NodeManager
        ActivationReference = { Definition = definition.Path; InstanceId = instanceId }
        ActorRef = if definition.IsLocal then None else Some(definition.GetActorRef(actor)) 
        Configuration = configuration
    }

    override __.DefinitionActivationResults = Seq.singleton {
        NodeManager = Cluster.NodeManager
        ActivationReference = { Definition = definition.Path; InstanceId = instanceId }
        Configuration = configuration
    }

    override a.Deactivate() = 
        a.DeactivateAsync() |> Async.RunSynchronously

    override a.DeactivateAsync() = async { 
        definition.LogInfo <| sprintf "Deactivating %d..." instanceId
        NodeRegistry.Registry.UnRegisterLocal { Definition = definition.Path; InstanceId = instanceId }
        disposable.Dispose()
        do! a.Definition.OnDeactivated(a.ActivationReference)
    }
    
    
and [<AbstractClass>] GroupDefinition(parent: DefinitionPath) =
    inherit Definition(parent)

    abstract Collocated: Definition list
    abstract Components: Definition list
    default def.Components = def.Collocated

    override def.Dependencies = 
        def.Collocated 
        |> List.map (fun comp -> comp.Dependencies)  
        |> List.concat

    override def.ActivateAsync(instanceId: int, configuration: ActivationConfiguration) = 
        revasync {
            def.LogInfo "Activating components..."

            let! activations =
                def.Collocated |> List.chooseRevAsync (fun compDef -> revasync {
                    let isNotActivated = NodeRegistry.Registry.TryResolveLocal { Definition = compDef.Path; InstanceId = instanceId } |> Option.isNone
                    if isNotActivated then
                        def.LogInfo (sprintf "Activating component %O..." compDef.Path)
                        let configuration' = compDef.Configuration.Override(configuration)
                        let! activation = compDef.ActivateAsync(instanceId, configuration')
                        return Some activation
                    else return None
                })

            NodeRegistry.Registry.RegisterDefinition {
                NodeManager = Cluster.NodeManager
                ActivationReference = { Definition = def.Path; InstanceId = instanceId }
                Configuration = configuration
            }

            let groupActivation = new GroupActivation(instanceId, activations, def, configuration) :> Activation

            return groupActivation
        }

    //TODO!!! Are there more events that need propagating?
    override def.OnActivated(activation: Activation) = revasync {
        match activation with
        | :? GroupActivation as groupActivation ->    
            for componentActivation in groupActivation.ComponentActivations do
                do! componentActivation.Definition.OnActivated(componentActivation)
        | _ -> ()
    }

    override def.OnDeactivate(activation: Activation) = async {
        match activation with
        | :? GroupActivation as groupActivation ->    
            for componentActivation in groupActivation.ComponentActivations do
                do! componentActivation.Definition.OnDeactivate(componentActivation)
        | _ -> ()
    }

and GroupActivation(instanceId: int, activations: seq<Activation>, definition: GroupDefinition, configuration: ActivationConfiguration) =
    inherit Activation(instanceId, definition)

    member internal __.ComponentActivations: seq<Activation> = activations

    override __.ActorActivationResults = activations |> Seq.collect (fun activation -> activation.ActorActivationResults)

    override __.DefinitionActivationResults = seq {
        yield {
            NodeManager = Cluster.NodeManager
            ActivationReference = { Definition = definition.Path; InstanceId = instanceId }
            Configuration = configuration
        }
        yield! activations |> Seq.collect (fun activation -> activation.DefinitionActivationResults)
    }

    override a.Deactivate() =  a.DeactivateAsync() |> Async.RunSynchronously
    override a.DeactivateAsync() = async { 
        definition.LogInfo "Deactivating..."
        for activation in activations do 
            definition.LogInfo <| sprintf "Deactivating component %O..." activation.ActivationReference
            do! activation.DeactivateAsync() 
        NodeRegistry.Registry.UnRegisterDefinition(Cluster.NodeManager, { Definition = definition.Path; InstanceId = instanceId })

        do! a.Definition.OnDeactivated(a.ActivationReference)
    }


and [<StructuralEquality; StructuralComparison>] DefinitionPath =
    | EmpComponent
    | NamedComponent of string * DefinitionPath
    with
        member path.IsEmpty = path = EmpComponent
        
        member path.Head =
            match path with
            | EmpComponent -> None
            | NamedComponent(defName, _) -> Some defName

        member path.IsDisjointFrom(other: DefinitionPath) =
            path.Head <> other.Head

        member path.Append(path': DefinitionPath) =
            match path with
            | EmpComponent -> path'
            | NamedComponent(name, path'') -> NamedComponent(name, path''.Append(path'))

        member path.IsAncestorOf(path': DefinitionPath) =
            match path, path' with
            | NamedComponent(name, rest), NamedComponent(name', rest') when name = name' -> rest.IsAncestorOf(rest')
            | EmpComponent, _ -> true
            | _ -> path = path'

        member path.BaseName =
            let rec baseName currentName path =
                match path with
                | EmpComponent -> currentName
                | NamedComponent(name, next) -> baseName name next

            match path with
            | EmpComponent -> ""
            | _ -> baseName "" path

        member path.Prefix =
            let rec prefixFold current acc =
                match current with
                | EmpComponent -> EmpComponent
                | NamedComponent(_, EmpComponent) -> acc
                | NamedComponent(name, next) -> prefixFold next (acc/name)

            prefixFold path EmpComponent

        member path.PostFixOf(path': DefinitionPath) =
            match path, path' with
            | NamedComponent(name, EmpComponent), NamedComponent(name', EmpComponent) when name = name' -> EmpComponent
            | NamedComponent(name, rest), NamedComponent(name', rest') when name = name' -> rest.PostFixOf(rest')
            | NamedComponent(name, rest), NamedComponent(_, _)
            | NamedComponent(name, rest), EmpComponent -> path
            | NamedComponent(name, rest), NamedComponent(name', EmpComponent) when name = name' -> path
            | EmpComponent, EmpComponent -> EmpComponent
            | _ -> invalidArg "path'" "Paths are diverging."

        member path.IsDescendantOf(path': DefinitionPath) = path'.IsAncestorOf(path)

        member path.Append(name: string) = path.Append(NamedComponent(name, EmpComponent))

        member path.Append(definition: Definition) = path.Append(definition.Name) //path.Append(LiteralComponent definition)

        member path.Prepend(path': DefinitionPath) = path'.Append(path)

        member path.Prepend(name: string) = path.Prepend(NamedComponent(name, EmpComponent))

        member path.Components =
            match path with
            | EmpComponent -> 0
            | NamedComponent(_, rest) -> 1 + rest.Components

        override path.ToString() =
            match path with
            | EmpComponent -> ""
            | NamedComponent(name, path') -> sprintf "/%s%O" name path'

        static member (/) (path: DefinitionPath, name: string) = path.Append(name)
        static member (/) (name: string, path: DefinitionPath) = path.Prepend(name)
        static member (/) (path: DefinitionPath, path': DefinitionPath) = path.Append(path')

        static member (/) (path: DefinitionPath, definition: Definition) = path.Append(definition)

        static member ListFormatter = 
            (fun () (ps: DefinitionPath list) -> 
                let rec printlist = function [] -> "" | i::[] -> i.ToString() | i::is -> i.ToString() + "; " + (printlist is)
                "[ " + (printlist ps) + " ]"
            )

and DefinitionRegistry = private {
    Registrations: Map<DefinitionPath, Definition>
} with
    static member Empty = { Registrations = Map.empty }

    member reg.Register(definition: Definition) =
        let reg' = { reg with Registrations = reg.Registrations |> Map.add definition.Path definition }
        match definition with
        | :? GroupDefinition as group ->
            group.Components |> List.fold (fun (reg'' : DefinitionRegistry) def -> reg''.Register def) reg'
        | _ -> reg'

    member reg.TryResolve(path: DefinitionPath) =
        reg.Registrations.TryFind path

    member reg.Resolve(path: DefinitionPath) =
        match reg.TryResolve path with
        | Some def -> def
        | None -> raise (new System.Collections.Generic.KeyNotFoundException(sprintf "No definition found by path %O" path))

    member reg.GetDirectDependantsOf(path: DefinitionPath) =
        reg.Registrations 
        |> Map.toSeq
        |> Seq.map snd
        |> Seq.filter (fun def -> def.Dependencies |> List.exists (fun dep -> dep.Definition = path))

and ClusterInfo = {
    //Invariant:
    //Master \not\in AltMasters
    Master: Address
    AltMasters: Address list
    ClusterId: ClusterId
} with //TODO!!! Extract these to a combinator
    member private __.SerializerName =
        Serialization.SerializerRegistry.GetDefaultSerializer().Name

    member cluster.ClusterManager : ReliableActorRef<ClusterManager> =
        ActorRef.fromUri <| sprintf "utcp://%O/*/clusterManager.%s/%s" cluster.Master cluster.ClusterId cluster.SerializerName
        |> ReliableActorRef.FromRef

    member cluster.MasterNode : ReliableActorRef<NodeManager> =
        ActorRef.fromUri <| sprintf "utcp://%O/*/nodeManager/%s" cluster.Master cluster.SerializerName
        |> ReliableActorRef.FromRef

    member cluster.ClusterStateLoggers : ReliableActorRef<ClusterStateLogger> list =
        cluster.AltMasters |> List.map (fun address ->
            ActorRef.fromUri <| sprintf "utcp://%O/*/clusterStateLogger.%s/%s" address cluster.ClusterId cluster.SerializerName
            |> ReliableActorRef.FromRef)

and ClusterId = string

and ManagedCluster = {
    ClusterManager: Actor<ClusterManager>
    AltMasters: Address list
    ClusterHealthMonitor: Actor<ClusterHealthMonitor>
} with
    member private __.SerializerName =
        Serialization.SerializerRegistry.GetDefaultSerializer().Name

    member cluster.ClusterStateLoggers =
        cluster.AltMasters |> List.map (fun address ->
            ActorRef.fromUri <| sprintf "utcp://%O/*/clusterStateLogger.HEAD/%s" address cluster.SerializerName
            |> ReliableActorRef.FromRef :> ActorRef<ClusterStateLogger>)

and NodeEventManager() =
    abstract OnInit: unit -> Async<unit>
    default __.OnInit() = async.Zero()

    abstract OnClusterInit: ClusterId * ActorRef<ClusterManager> -> Async<unit>
    default __.OnClusterInit(_, _) = async.Zero()

    abstract OnAddToCluster: ClusterId * ReliableActorRef<ClusterManager> -> Async<unit>
    default __.OnAddToCluster(_, _) = async.Zero()

    abstract OnAttachToCluster: ClusterId * ReliableActorRef<ClusterManager> -> Async<unit>
    default __.OnAttachToCluster(_, _) = async.Zero()

    abstract OnRemoveFromCluster: ClusterId -> Async<unit>
    default __.OnRemoveFromCluster _ = async.Zero()

    abstract OnSystemFault: unit -> Async<unit>
    default __.OnSystemFault() = async.Zero()

    abstract OnMaster: ClusterId -> Async<unit>
    default __.OnMaster(_) = async.Zero()

    abstract OnMasterRecovered: ClusterId -> Async<unit>
    default __.OnMasterRecovered(_) = async.Zero()
    
and ActivationPattern = {
    ClusterId: ClusterId
    PatternF: BehaviorContext<NodeManager> -> ActorRef<ClusterManager> -> ActivationReference -> ClusterActivation [] * ClusterActiveDefinition [] -> (int * ActivationConfiguration -> RevAsync<Activation>) option
}

and NodeConfiguration = {
    NodeAddress: Address
    DefinitionRegistry: DefinitionRegistry
    EventManager: NodeEventManager
    ActivationPatterns: ActivationPattern list
}

and NodeState = {
    //Invariant:
    //\exists info. ClusterInfo = Some info => Address <> info.Master && \exists h. HeartBeat = Some h
    //\forall cluster. (cluster \in ManagedClusters && Address = cluster.ClusterManager.Ref.(Id :?> TcpActorId).Address)
    //ClusterStateLogger = Some clusterStateLogger => ClusterInfo = Some info && clusterStateLogger.Ref \in info.ClusterStateLoggers
    Address: Address
    NodeRegistry: NodeRegistry
    DefinitionRegistry: DefinitionRegistry
    ActivationsMap: Map<ActivationReference, Activation>
    ClusterStateLogger: Actor<ClusterStateLogger> option
    ClusterInfo: ClusterInfo option //Info about the cluster this node participates in
    ManagedClusters: Map<ClusterId, ManagedCluster> //The clusters this node is managing 
    FinalizedClusterManagers: IDisposable list
    EventManager: NodeEventManager
    ActivationPatterns: ActivationPattern list
    HeartBeat: IDisposable option
    NodeEventExecutor: Actor<NodeEventExecutor>
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
        Cluster.NodeManager

    member state.ResolveActivation(activationReference: ActivationReference) =
        match state.ActivationsMap.TryFind activationReference with
        | Some activation -> activation
        | None -> 
            if not activationReference.Definition.Prefix.IsEmpty then
                state.ResolveActivation { Definition = activationReference.Definition.Prefix; InstanceId = activationReference.InstanceId }
            else raise <| new System.Collections.Generic.KeyNotFoundException("Unable to find activation for given activation reference")

    static member New(nodeConfig: NodeConfiguration) = {
        Address = nodeConfig.NodeAddress
        NodeRegistry = NodeRegistry.Registry
        DefinitionRegistry = nodeConfig.DefinitionRegistry
        ActivationsMap = Map.empty
        ClusterStateLogger = None
        ClusterInfo = None
        ManagedClusters = Map.empty
        FinalizedClusterManagers = []
        EventManager = nodeConfig.EventManager
        ActivationPatterns = nodeConfig.ActivationPatterns
        HeartBeat = None
        NodeEventExecutor = NodeState.CreateNodeEventExecutorActor()
    }

    static member CreateNodeEventExecutorActor() =
        Actor.spawn (fun msg -> async {
            match msg with
            | ExecEvent(ctx, handler) ->
                try
                    do! handler
                with e -> ctx.LogError e
            | ExecSync(R reply) -> reply nothing
        })
        |> Actor.start

and NodeEventExecutor =
    | ExecEvent of BehaviorContext<NodeManager> * Async<unit>
    | ExecSync of IReplyChannel<unit>

and ClusterActiveDefinition = {
    NodeManager: ActorRef<NodeManager>
    ActivationReference: ActivationReference
    Configuration: ActivationConfiguration
} with
    static member New(nodeManager: ActorRef<NodeManager>, activationRef: ActivationReference, conf: ActivationConfiguration) = {
        NodeManager = nodeManager
        ActivationReference = activationRef
        Configuration = conf
    }

    static member MakeKey(nodeManager: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Key.composite [| nodeManager; activationRef |]

    member cd.Key = ClusterActiveDefinition.MakeKey(cd.NodeManager, cd.ActivationReference)

    static member RegisterBatch (registrations: seq<ClusterActiveDefinition>) (table: Table<ClusterActiveDefinition>) =
        registrations |> Table.insertValues table

    static member UnRegister (node: ActorRef<NodeManager>) (activationRef: ActivationReference) (table: Table<ClusterActiveDefinition>) =
        Table.remove (ClusterActivation.MakeKey(node, activationRef)) table

    static member UnRegisterActivationRef (activationRef: ActivationReference) (table: Table<ClusterActiveDefinition>) =
        Table.delete <@ fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference = activationRef @> table

    static member UnRegisterActivationRefDescendants (activationRef: ActivationReference) (table: Table<ClusterActiveDefinition>) =
        Table.delete <@ fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference.Definition.IsDescendantOf activationRef.Definition
                                                       && clusterActiveDefinition.ActivationReference.InstanceId = activationRef.InstanceId @> table

    static member ResolveActiveDefinitions (activationRef: ActivationReference) (table: Table<ClusterActiveDefinition>) =
        Query.from table
        |> Query.where <@ fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference = activationRef @>
        |> Query.toSeq

    static member ResolveActiveDefinitionTree (activationRef: ActivationReference) (table: Table<ClusterActiveDefinition>) =
        Query.from table
        |> Query.where <@ fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference.Definition.IsDescendantOf activationRef.Definition
                                                         && clusterActiveDefinition.ActivationReference.InstanceId = activationRef.InstanceId @>
        |> Query.toSeq

and ClusterActivation = {
    NodeManager: ActorRef<NodeManager>
    ActivationReference: ActivationReference
    Configuration: ActivationConfiguration
    ActorRef: ActorRef option
} with
    static member MakeKey(nodeManager: ActorRef<NodeManager>, activationRef: ActivationReference) =
        Key.composite [| nodeManager; activationRef |]

    member ca.Key = ClusterActivation.MakeKey(ca.NodeManager, ca.ActivationReference)

    static member RegisterBatch (registrations: seq<ClusterActivation>) (table: Table<ClusterActivation>) =
        registrations |> Table.insertValues table

    static member RegisterBatchOverriding (registrations: seq<ClusterActivation>) (table: Table<ClusterActivation>) =
        registrations |> Seq.fold (fun tbl activationResult ->
            match tbl.DataMap.TryFind activationResult.Key with
            | None -> Table.insert activationResult tbl
            | Some existing when existing.ActorRef <> activationResult.ActorRef -> Table.insert activationResult tbl
            | _ -> tbl
        ) table

    static member Register (registration: ClusterActivation) (table: Table<ClusterActivation>) = 
        ClusterActivation.RegisterBatch (Seq.singleton registration) table

    static member UnRegister (node: ActorRef<NodeManager>) (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        Table.remove (ClusterActivation.MakeKey(node, activationRef)) table

    static member UnRegisterActivationRef (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        Table.delete <@ fun clusterActivation -> clusterActivation.ActivationReference = activationRef @> table

    static member UnRegisterActivationRefDescendants (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        Table.delete <@ fun clusterActivation -> clusterActivation.ActivationReference.Definition.IsDescendantOf activationRef.Definition
                                                 && clusterActivation.ActivationReference.InstanceId = activationRef.InstanceId @> table

    ///Returns a sequence of cluster activations of the given activation reference
    static member ResolveActivation (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        Query.from table
        |> Query.where <@ fun ca -> ca.ActivationReference = activationRef @>
        |> Query.toSeq

    static member ResolveDefinitionActivation (definitionPath: DefinitionPath) (table: Table<ClusterActivation>) =
        Query.from table
        |> Query.where <@ fun ca -> ca.ActivationReference.Definition = definitionPath @>
        |> Query.toSeq

    static member ResolveNodeActivation (activationRef: ActivationReference) (node: ActorRef<NodeManager>) (table: Table<ClusterActivation>) =
        Query.from table
        |> Query.where <@ fun ca -> ca.ActivationReference = activationRef && ca.NodeManager = node @>
        |> Query.toSeq
        |> Seq.tryHead

    static member ResolveDependants (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        Query.from table
        |> Query.where <@ fun ca -> ca.ActivationReference.Definition.IsAncestorOf activationRef.Definition 
                                    && ca.ActivationReference.InstanceId = activationRef.InstanceId @>
        |> Query.toSeq

    static member ResolveExact<'T> (activationRef: ActivationReference) (table: Table<ClusterActivation>) =
        ClusterActivation.ResolveActivation activationRef table
        |> Seq.map (fun ca -> ca.ActorRef)
        |> Seq.choose id
        |> Seq.map (fun actorRef -> actorRef :?> ActorRef<'T>)

and ClusterNode = {
    NodeManager: ActorRef<NodeManager>
    TimeAdded: DateTime
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
    HealthMonitor: ActorRef<ClusterHealthMonitor> option
} with

    override state.ToString() =
        let newline = System.Environment.NewLine
        sprintf 
            "ClusterState :: %s 
             ClusterId = %A %s 
             MasterNode = %A %s
             Db :: %s
             %O"
             newline
             state.ClusterId newline
             state.MasterNode newline
             newline
             state.Db

    static member New(clusterId: ClusterId, masterNode: ActorRef<NodeManager>, timeSpanToDeathDeclaration: TimeSpan, definitionRegistry: DefinitionRegistry) = {
        ClusterId = clusterId
        MasterNode = masterNode
        Db = ClusterDb.Empty
        TimeSpanToDeathDeclaration = timeSpanToDeathDeclaration
        DefinitionRegistry = definitionRegistry
        HealthMonitor = None
    }

    member state.Master = 
        (state.MasterNode.[UTCP].Id :?> TcpActorId).Address

    //These are all the slave nodes
    member state.Nodes =
        Query.from state.Db.ClusterNode
        |> Query.toSeq
        |> Seq.map (fun node -> node.NodeManager)

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

//    member state.DeadNodes = 
//        Query.from state.Db.ClusterNode
//        |> state.DeadNodesQueryFilter
//        |> Query.toSeq
//        |> Seq.map (fun node -> node.NodeManager)

    member state.AddNode(nodeManager: ActorRef<NodeManager>): ClusterState =
        { state with Db = state.Db |> Database.insert <@ fun db -> db.ClusterNode @> { NodeManager = nodeManager; TimeAdded = DateTime.Now } }

    member state.RemoveFromNodes(nodeManager: ActorRef<NodeManager>): ClusterState =
        { state with Db = state.Db |> Database.remove <@ fun db -> db.ClusterNode @> nodeManager }

    member state.RemoveNode(nodeManager: ActorRef<NodeManager>): ClusterState =
        let state' =
            { state with
                Db = ((((state.Db |> Database.remove <@ fun db -> db.ClusterNode @> nodeManager)
                              |> Database.remove <@ fun db -> db.ClusterAltMasterNode @> nodeManager)
                              |> Database.delete <@ fun db -> db.ClusterActivation @> <@ fun ca -> ca.NodeManager = nodeManager @>)
                              |> Database.delete <@ fun db -> db.ClusterActiveDefinition @> <@ fun cd -> cd.NodeManager = nodeManager @>)
            }

        assert (
            if state.Db.ClusterNode.DataMap |> Map.toList |> List.map snd |> List.exists (fun cn -> cn.NodeManager = nodeManager) then
                state'.Db.ClusterNode.DataMap |> Map.toList |> List.map snd |> List.forall (fun cn -> cn.NodeManager <> nodeManager)
            else true
        )

        state'

    member state.SetTimeSpanToDeathDeclaration(timeSpan: TimeSpan): ClusterState =
        { state with TimeSpanToDeathDeclaration = timeSpan }

    member state.AddActivation(clusterActivation: ClusterActivation): ClusterState =
        { state with Db = state.Db |> Database.insert <@ fun db -> db.ClusterActivation @> clusterActivation }

    member state.AddActiveDefinition(clusterActiveDefinition: ClusterActiveDefinition): ClusterState =
        { state with Db = state.Db |> Database.insert <@ fun db -> db.ClusterActiveDefinition @> clusterActiveDefinition }

    member state.DeActivateDefinition(activationRef: ActivationReference): ClusterState =
        { state with Db = state.Db |> Database.delete <@ fun db -> db.ClusterActivation @> <@ fun ca -> ca.ActivationReference.Definition.IsDescendantOf activationRef.Definition 
                                                                                                        && ca.ActivationReference.InstanceId = activationRef.InstanceId @> 
                                   |> Database.delete <@ fun db -> db.ClusterActiveDefinition @> <@ fun cd -> cd.ActivationReference.Definition.IsDescendantOf activationRef.Definition 
                                                                                                              && cd.ActivationReference.InstanceId = activationRef.InstanceId @> }

    member state.GetActiveDefinitionsInCluster(activationRef: ActivationReference) =
        Query.from state.Db.ClusterActiveDefinition
        |> Query.innerJoin state.Db.ClusterNode <@ fun (clusterActiveDefinition, clusterNode) -> clusterActiveDefinition.NodeManager = clusterNode.NodeManager @>
        |> Query.where <@ fun (clusterActiveDefinition, _) -> clusterActiveDefinition.ActivationReference = activationRef @>
        |> Query.toSeq
        |> Seq.map fst
        |> Seq.append (
            Query.from state.Db.ClusterActiveDefinition
            |> Query.where <@ fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference = activationRef
                                                             && clusterActiveDefinition.NodeManager = state.MasterNode @>
            |> Query.toSeq
        )

    member state.GetActivationInfo(activationRef: ActivationReference) =
        Query.from state.Db.ClusterActivation
        |> Query.where <@ fun ca -> ca.ActivationReference = activationRef @>
        |> Query.toSeq,
        Query.from state.Db.ClusterActiveDefinition
        |> Query.where <@ fun cad -> cad.ActivationReference = activationRef @>
        |> Query.toSeq

    member state.IsActivated(activationRef: ActivationReference) =
        Query.from state.Db.ClusterActiveDefinition
        |> Query.where <@ fun cad -> cad.ActivationReference = activationRef @>
        |> Query.toSeq
        |> Seq.isEmpty 
        |> not

    member state.GetAllActiveDefinitions() =
        Query.from state.Db.ClusterActiveDefinition
        |> Query.toSeq

    member state.AddAltMasterNode(clusterAltMasterNode: ClusterAltMasterNode): ClusterState =
        if state.Db.ClusterNode.DataMap.ContainsKey clusterAltMasterNode.NodeManager then
            { state with Db = state.Db |> Database.insert <@ fun db -> db.ClusterAltMasterNode @> clusterAltMasterNode }
        else invalidArg "clusterAltMasterNode" "Given node is not in the cluster database."

    member state.RemoveAltMasterNode(nodeManager: ActorRef<NodeManager>): ClusterState =
        let state' = { state with Db = state.Db |> Database.remove <@ fun db -> db.ClusterAltMasterNode @> nodeManager }

        assert (
            if state.Db.ClusterAltMasterNode.DataMap |> Map.toList |> List.map snd |> List.exists (fun cn -> cn.NodeManager = nodeManager) then
                state'.Db.ClusterAltMasterNode.DataMap |> Map.toList |> List.map snd |> List.forall (fun cn -> cn.NodeManager <> nodeManager)
            else true
        )

        state'

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
    //Same as above, but also return all activation results, if boolean is false
    //dependencies are not automatically activated
    | ActivateDefinitionWithResults of IReplyChannel<ClusterActivation [] * ClusterActiveDefinition []> * bool * ActivationRecord * ClusterActivation [] * ClusterActiveDefinition []
    //DeActivateDefinition(ch, activationReference)
    //Throws
    //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
    //OutOfNodesException => unable to recover due to lack of node;; SYSTEM FAULT
    //SystemFailureException => Global system failure;; SYSTEM FAULT
    | DeActivateDefinition of IReplyChannel<unit> * ActivationReference
    //resolvedActorRefs = ResolveActivationRefs(activationReference)
    //Throws
    //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
    | ResolveActivationRefs of IReplyChannel<ActorRef []> * ActivationReference
    //resolvedClusterActivations = ResolveActivationInstances(definitionPath)
    //Throws
    //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
    | ResolveActivationInstances of IReplyChannel<ClusterActivation []> * DefinitionPath
    | RemoveNode of ActorRef<NodeManager>
    | DetachNodes of ActorRef<NodeManager>[]
    | AddNode of ActorRef<NodeManager>
    | AddNodeSync of IReplyChannel<unit> * ActorRef<NodeManager>
    | GetAltNodes of IReplyChannel<ActorRef<NodeManager>[]>
    | GetAllNodes of IReplyChannel<ActorRef<NodeManager>[]>
    //| HeartBeat of ActorRef<NodeManager>
    | KillCluster
    | KillClusterSync of IReplyChannel<unit>
    | FailCluster of exn
    //| HealthCheck

and ClusterHealthMonitorState = {
    Nodes: Map<Address, DateTime>
    HeartBeatInterval: TimeSpan
    MinimumHeartBeatInterval: TimeSpan
    AdaptationFactor: float
    DetectionFactor: float
    ParallelizationFactor: int //after how many nodes should health checking be parallelized
    Enabled: bool
} with 
    static member Default = {
        Nodes = Map.empty
        HeartBeatInterval = TimeSpan.FromSeconds 5.0
        MinimumHeartBeatInterval = TimeSpan.FromSeconds 3.0
        AdaptationFactor = 1.5
        DetectionFactor = 2.0
        ParallelizationFactor = 100
        Enabled = true
    }

and ClusterHealthMonitor =
    | HeartBeat of Address
    | StartMonitoringNode of Address
    | StopMonitoringNode of Address
    | EnableMonitoring
    | DisableMonitoring

and ClusterStateUpdate =
    | ClusterAddNode of ActorRef<NodeManager>
    | ClusterRemoveNode of ActorRef<NodeManager>
    | ClusterRemoveFromNodes of ActorRef<NodeManager>
    | ClusterSetTimeSpanToDeathDeclaration of TimeSpan
    | ClusterActivation of ClusterActivation
    | ClusterActiveDefinition of ClusterActiveDefinition
    | ClusterDeActivateDefinition of ActivationReference
    | ClusterAddAltMaster of ClusterAltMasterNode
    | ClusterRemoveAltMaster of ActorRef<NodeManager>

and ClusterUpdate = { 
    NodeInsertions: ActorRef<NodeManager> []
    NodeRemovals: ActorRef<NodeManager> []
    SimpleNodeRemovals: ActorRef<NodeManager> []
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
            SimpleNodeRemovals = updates |> Seq.choose (function ClusterRemoveFromNodes node -> Some node | _ -> None) |> Seq.toArray
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
              |> fold (fun s n -> s.RemoveFromNodes n) update.SimpleNodeRemovals
              |> fold (fun s a -> s.AddActivation a) update.Activations
              |> fold (fun s d -> s.AddActiveDefinition d) update.DefinitionActivations
              |> fold (fun s p -> s.DeActivateDefinition p) update.DefinitionDeActivations
              |> fold (fun s n -> s.AddAltMasterNode n) update.AltMasterAddittions
              |> fold (fun s n -> s.RemoveAltMasterNode n) update.AltMasterRemovals
              |> (fun ts (s: ClusterState) -> match ts with Some t -> s.SetTimeSpanToDeathDeclaration t | None -> s) update.TimeSpanToDeath
        

and ClusterStateLogger =
    | UpdateClusterState of ClusterUpdate
    | GetClusterState of IReplyChannel<ClusterState>


and ClusterConfiguration = {
    ClusterId: ClusterId
    Nodes: ActorRef<NodeManager> []
    ReplicationFactor: int
    FailoverFactor: int
    NodeDeadNotify: ActorRef<NodeManager> -> Async<unit>
} with
    static member New(clusterId: ClusterId) = {
        ClusterId = clusterId
        Nodes = Array.empty
        ReplicationFactor = 0
        FailoverFactor = 0
        NodeDeadNotify = fun node -> Cluster.ClusterManager <-!- RemoveNode node
    }

and NodeType = Master | Slave | Idle

and NodeManager =
    //activationResults = Activate(ch, activationRef, config, dependencyActivations)
    //Throws
    //KeyNotFoundException => defPath unknown in node
    //PartialActivationException => failure in activation could not be recovered to full
    //ActivationFailureException => activation failed
    | Activate of IReplyChannel<ClusterActivation[] * ClusterActiveDefinition[]> * ActivationReference * ActivationConfiguration * (ClusterActivation []) * (ClusterActiveDefinition [])
    //DeActivate(ch, activationRef, throwIfDefPathNotExists)
    //Throws
    //(throwIfDefPathNotExists => KeyNotFoundException) => defPath not found in node
    //e => exception e thrown in deactivation
    | DeActivate of IReplyChannel<unit> * ActivationReference * bool
    //actorRef = Resovle(ch, activationRef)
    //Throws
    //KeyNotFoundException => no activation found
    | Resolve of IReplyChannel<ActorRef> * ActivationReference
    | TryResolve of IReplyChannel<ActorRef option> * ActivationReference
    //Get a managed cluster by id
    //clusterManager = TryGetManagedCluster(clusterId)
    //Throws ;; nothing
    | TryGetManagedCluster of IReplyChannel<ActorRef<ClusterManager> option> * ClusterId
    //newClusterStateLogger = AssumeAltMaster(ch, currentClusterState)
    //Throws
    //InvalidOperationException => already a master node
    | AssumeAltMaster of IReplyChannel<ActorRef<ClusterStateLogger>> * ClusterState
    //NotifyDependencyLost(dependant, dependency, lostNode)
    | NotifyDependencyLost of ActivationReference[] * ActivationReference * ActorRef<NodeManager>
    //NotifyDependencyRecovered(dependant, dependency)
    | NotifyDependencyRecovered of ActivationReference[] * ActivationReference * ClusterActivation[] * ClusterActiveDefinition[]
    | TriggerSystemFault
    | MasterNodeLoss
    | AttachToCluster of ClusterInfo
    | AttachToClusterSync of IReplyChannel<unit> * ClusterInfo
    | StopMonitoring of ClusterId
    | StartMonitoring of ClusterId
    | FiniCluster of ClusterId
    | FiniClusterSync of IReplyChannel<unit> * ClusterId
    | DisposeFinalizedClusters
    | DetachFromCluster
    | UpdateAltMasters of Address []
    //Initialize a cluster
    //altMasterAddresses = InitCluster(ch, clusterConfiguration)
    //Throws
    //ClusterInitializedException => cluster already initialized
    | InitCluster of IReplyChannel<Address[]> * ClusterConfiguration
    //replies when pending node events are processed
    //Throws ;; nothing
    | SyncNodeEvents of IReplyChannel<unit>
    //Throws
    //SystemCorruptionException => SYSTEM FAULT
    | GetNodeType of IReplyChannel<NodeType>
    with
        static member ActivateRevAsync(actorRef: ActorRef<NodeManager>, 
                                       activationRef: ActivationReference, 
                                       configuration: ActivationConfiguration, 
                                       dependencyActivations: ClusterActivation[], 
                                       dependencyActiveDefinitions: ClusterActiveDefinition [], 
                                       ?ctx: BehaviorContext<_>) =
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            //FaultPoint
            //MessageHandlingException KeyNotFoundException => definition.Path not found in node;; do not handle allow to fail
            //MessageHandlingException PartialActivationException => failed to properly recover an activation failure;; do not handle allow to fail
            //MessageHandlingException ActivationFailureException => failed to activate definition.Path;; do not handle allow to fail
            //FailureException => Actor or Node level failure ;; do not handle allow to fail
            let computation = actorRef <!- fun ch -> Activate(ch, activationRef, configuration, dependencyActivations, dependencyActiveDefinitions)
            let recovery (_, activated: ClusterActiveDefinition[]) = async {
                for activationResult in activated do
                    try
                        if ctx.IsSome then ctx.Value.LogInfo <| sprintf "Deactivating %O ..." activationResult.ActivationReference.Definition

                        //FaultPoint
                        //MessageHandlingException => some exception thrown by deactivation;; allow to fail
                        //FailureException => Actor or Node level failure;; TRIGGER NODE FAILURE??? //TODO!!! Check on how to trigger node failure here
                        do! actorRef <!- fun ch -> DeActivate(ch, activationResult.ActivationReference, false)

                        if ctx.IsSome then ctx.Value.LogInfo <| sprintf "%O deactivated." activationResult.ActivationReference.Definition
                    with FailureException _ ->
                        //TODO!!! Perhaps log failure
                        ()
            }
            RevAsync.FromAsync(computation, recovery)

and NodeHeartBeat = 
    | SetHeartBeatInterval of TimeSpan
    | BeatBack of IReplyChannel<unit>
    | StopBeating
    | StartBeating of TimeSpan

and Raw<'T> = Nessos.Thespian.PowerPack.Raw<'T>

and RawProxy =
    | PostRaw of Raw<obj>
    | PostWithReplyRaw of IReplyChannel<Raw<obj>> * Raw<obj>

and ClusterProxyManager =
    | ForwardMessage of ActivationReference * Raw<obj>
    | ForwardMessageSync of IReplyChannel<unit> * ActivationReference * Raw<obj>
    | ForwardMessageWithReply of IReplyChannel<Raw<obj>> * ActivationReference * Raw<obj>
    | ForwardDirect of ActivationReference * RawProxy
    | RegisterProxy of ActivationReference * ActorRef<RawProxy>
    | UnregisterProxy of ActivationReference

and Cluster() =
    static let customRegistry = Atom.atom Map.empty
    static let nodeManager = ref None : ActorRef<NodeManager> option ref
    static let clusterManager = ref None : ReliableActorRef<ClusterManager> option ref
    static let definitionRegistry = ref None : DefinitionRegistry option ref

    static let onNodeManagerSet = new Event<ActorRef<NodeManager> option>()

    [<CLIEvent>]
    static member OnNodeManagerSet = onNodeManagerSet.Publish

    static member NodeManager = nodeManager.Value.Value
    static member ClusterManager = clusterManager.Value.Value

    static member NodeRegistry = NodeRegistry.Registry
    
    /// The current node's DefinitionRegistry
    static member DefinitionRegistry = 
        if definitionRegistry.Value.IsNone then raise <| new System.Collections.Generic.KeyNotFoundException("Definition registry not initialized.")
        definitionRegistry.Value.Value

    static member Set<'T>(key: string, value: 'T) =
        Atom.swap customRegistry (Map.add key (box value))

    static member TryGet<'T>(key: string) : 'T option =
        customRegistry.Value.TryFind key |> Option.map unbox

    static member Get<'T>(key: string) =
        match Cluster.TryGet<'T>(key) with
        | Some v -> v
        | None -> raise <| new System.Collections.Generic.KeyNotFoundException("No item with specified key found in Cluster custom registry.")

    static member Get<'T>(key: string, timeout: int) =
        let rec getLoop () = async {
            match Cluster.TryGet<'T>(key) with
            | Some v -> return v
            | None ->
                do! Async.Sleep 500
                return! getLoop()
        }

        Async.RunSynchronously(getLoop(), timeout)

    static member internal SetNodeManager nodeManager' = 
        nodeManager := nodeManager'
        onNodeManagerSet.Trigger(nodeManager')
    static member internal SetClusterManager clusterManager' = clusterManager := clusterManager'
    static member internal SetDefinitionRegistry definitionRegistry' = definitionRegistry := definitionRegistry'

exception ClusterStateLogBroadcastException of string * ActorRef<NodeManager> list
exception ClusterStateLogFailureException of string

[<AutoOpen>]
module Utils =
    let empDef = EmpComponent
