module Nessos.Thespian.Cluster.ClusterManager

open System
open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.ImemDb
open Nessos.Thespian.AsyncExtensions
open Nessos.Thespian.Reversible
open Nessos.Thespian.Cluster

type private LogLevel = Nessos.Thespian.LogLevel

open Nessos.Thespian.Cluster.BehaviorExtensions.FSM

let private updateState (state: ClusterState) (updates: seq<ClusterStateUpdate>) =
    updates |> Seq.fold (fun (state: ClusterState) update ->
        match update with
        | ClusterAddNode node -> state.AddNode node
        | ClusterRemoveNode node -> state.RemoveNode node
        | ClusterRemoveFromNodes node -> state.RemoveFromNodes node
        | ClusterSetTimeSpanToDeathDeclaration timeSpan -> state.SetTimeSpanToDeathDeclaration timeSpan
        | ClusterActivation ca -> state.AddActivation ca
        | ClusterActiveDefinition cd -> state.AddActiveDefinition cd
        | ClusterDeActivateDefinition defPath -> state.DeActivateDefinition defPath
        | ClusterAddAltMaster am -> state.AddAltMasterNode am
        | ClusterRemoveAltMaster n -> state.RemoveAltMasterNode n
    ) state

//TODO!!! Check if instanceid should be included in the check
let private checkCyclicDependencies (definitionRegistry: DefinitionRegistry) (defPath: DefinitionPath) =
    let rec isCyclic (visited: Set<DefinitionPath>) (current: DefinitionPath) =
        if Set.contains current visited then true
        else
            let definition = definitionRegistry.Resolve current
            definition.Dependencies 
            |> List.exists (fun activationRecord -> isCyclic (Set.add current visited) activationRecord.Definition)

    isCyclic Set.empty defPath

let private checkAcyclicDependencies registry defPath = not (checkCyclicDependencies registry defPath)

let private tempUpdateStateActivations (activations: ClusterActivation list, activeDefinitions: ClusterActiveDefinition list) (state: ClusterState) =
    { state with Db = state.Db |> List.foldBack (Database.insert <@ fun db -> db.ClusterActivation @>) activations
                               |> List.foldBack (Database.insert <@ fun db -> db.ClusterActiveDefinition @>) activeDefinitions }

let activationStateUpdates (activations: ClusterActivation list, activeDefinitions: ClusterActiveDefinition list) =
    seq {
        yield! activations |> Seq.map ClusterActivation
        yield! activeDefinitions |> Seq.map ClusterActiveDefinition
    }

type ActivationTree =
    | SuspendedNode of ActivationReference
    | ExternalNode of ActivationReference
    | Node of ActivationRecord * Definition * (ActivationTree list)

let rec private getExternalDependencies (ctx: BehaviorContext<_>) (state: ClusterState) (activationRecord: ActivationRecord) =
    let definition = state.DefinitionRegistry.Resolve activationRecord.Definition
    let dependencies = definition.GetDependencies(activationRecord.Configuration)

    let externalsOfCollocated = 
        dependencies 
        |> Seq.filter (fun dep -> dep.IsCollocated)
        |> Seq.collect (getExternalDependencies ctx state)
        |> Seq.map (fun dep -> if dep.Instance.IsNone then { dep with Instance = activationRecord.Instance } else dep)
        |> Seq.distinct

    dependencies
    |> Seq.filter (fun dep -> not dep.IsCollocated)
    |> Seq.distinct
    |> Seq.map (fun dep -> if dep.Instance.IsNone then { dep with Instance = activationRecord.Instance } else dep)
    |> Seq.append externalsOfCollocated

//TODO!! Code duplication
let rec private getExternalDependencies2 (ctx: BehaviorContext<_>) (state: ClusterState) (activationRef: ActivationReference) =
    let definition = state.DefinitionRegistry.Resolve activationRef.Definition
    let dependencies = definition.Dependencies

    let externalsOfCollocated = 
        dependencies 
        |> Seq.filter (fun dep -> dep.IsCollocated)
        |> Seq.collect (getExternalDependencies ctx state)
        |> Seq.map (fun dep -> if dep.Instance.IsNone then { dep with Instance = Some activationRef.InstanceId } else dep)
        |> Seq.distinct

    dependencies
    |> Seq.filter (fun dep -> not dep.IsCollocated)
    |> Seq.distinct
    |> Seq.map (fun dep -> if dep.Instance.IsNone then { dep with Instance = Some activationRef.InstanceId } else dep)
    |> Seq.append externalsOfCollocated

let private computeActivationTree (ctx: BehaviorContext<_>) (state: ClusterState) 
                                  (activationRecord: ActivationRecord) =
    let rec computeActivationTree' (visited: Set<ActivationReference>) (activationRecord: ActivationRecord) =
        let activationRef = { Definition = activationRecord.Definition; InstanceId = activationRecord.Instance.Value }
        if visited |> Set.contains activationRef then 
            (SuspendedNode activationRef), visited
        else
            let definition = state.DefinitionRegistry.Resolve activationRecord.Definition

            let externalDependencies = getExternalDependencies ctx state activationRecord |> Seq.toList

            let visited', dependencyNodes =
                List.foldBack (fun dependency (visited'', nodes) -> 
                                    let depActivationRef = ActivationRecord.ToActivationReference dependency
                                    if state.IsActivated depActivationRef then
                                        visited'' |> Set.add depActivationRef, (ExternalNode depActivationRef)::nodes
                                    else
                                        let dependency' = { dependency with Configuration = dependency.Configuration.Override(activationRecord.Configuration) }
                                        let node, visited''' = computeActivationTree' visited'' dependency' in (visited'' |> Set.union visited'''), node::nodes)
                              externalDependencies
                              (visited, [])

            Node(activationRecord, definition, dependencyNodes), visited' |> Set.add activationRef

    computeActivationTree' Set.empty activationRecord |> fst



let rec executeActivationTree (ctx: BehaviorContext<_>)
                              (state: ClusterState)
                              (activationTree: ActivationTree) =

    let rec executeActivationTree' (activationTree: ActivationTree) = revasync {
        match activationTree with
        | SuspendedNode activationRef ->
            ctx.LogInfo <| sprintf "Suspending activation of %O" activationRef
            return ([], []), Some (suspendedNodeActivation ctx activationRef)
        | ExternalNode activationRef ->
            ctx.LogInfo <| sprintf "%O is already activated. Resolving activation info..." activationRef
            let externalActivations, externalActiveDefinitions = state.GetActivationInfo activationRef
            return (Seq.toList externalActivations, Seq.toList externalActiveDefinitions), None
        | Node(activationRecord, definition, dependencyNodes) ->
            ctx.LogInfo <| sprintf "%O :: Activating external dependencies..." activationRecord.Definition
            let! dependencyActivationResults = dependencyNodes |> Seq.map executeActivationTree' |> RevAsync.Parallel
            
            let suspendedActivationDependencies =
                dependencyActivationResults |> Seq.choose (function _, Some sna -> Some sna | _ -> None)

            //TODO!!! Improve this. Do not use so many intermediate structures
            let dependencyActivations, dependencyActiveDefinitions =
                dependencyActivationResults 
                |> Seq.choose (function r, None -> Some r | _ -> None)
                |> Seq.fold (fun (adas, adads) (das, dads) -> das@adas, dads@adads) ([], [])

            if Seq.isEmpty suspendedActivationDependencies then
                ctx.LogInfo <| sprintf "%O :: External dependencies activated." activationRecord.Definition
                //ctx.LogInfo <| sprintf "%O :: External dependency activations: %a" activationRecord.Definition DefinitionPath.ListFormatter (dependencyActivations |> List.map (fun dad -> dad.ActivationReference.Definition))
                let! results = activateNode ctx activationRecord definition state (List.toArray dependencyActivations, List.toArray dependencyActiveDefinitions)

                return results, None
            else
                let suspendedNodeActivation dependencies = revasync {
                    ctx.LogInfo "Resuming suspended activations..."
                    
                    let! results = suspendedActivationDependencies |> Seq.toList |> List.mapRevAsync (fun act -> act dependencies)
                    let dependencyActivations', dependencyActiveDefinitions' = 
                        results |> List.fold (fun (adas, adads) (das, dads) -> das@adas, dads@adads) ([], [])

                    let dependencies' = 
                        List.toArray (dependencyActivations'@dependencyActivations),
                        List.toArray (dependencyActiveDefinitions'@dependencyActiveDefinitions)

                    ctx.LogInfo <| sprintf "Resuming suspended activation of %O..." activationRecord.Definition

                    return! activateNode ctx activationRecord definition state dependencies'
                }
                return (dependencyActivations, dependencyActiveDefinitions), Some suspendedNodeActivation
    }

    revasync {
        let! dependencyActivations, suspsendNodesActivations = executeActivationTree' activationTree

        match suspsendNodesActivations with
        | Some act ->
            let! sacts, sdacts = act dependencyActivations
            let acts, dacts = dependencyActivations
            return sacts@acts, sdacts@dacts
        | None ->
            return dependencyActivations
    }

and suspendedNodeActivation (ctx: BehaviorContext<_>)
                            (activationRef: ActivationReference) 
                            (activations: ClusterActivation list, activeDefinitions: ClusterActiveDefinition list) =
    revasync {
        let relevantActivations = activations |> List.filter (fun ca -> ca.ActivationReference = activationRef)
        let relevantActiveDefinitions = activeDefinitions |> List.filter (fun cad -> cad.ActivationReference = activationRef)

        ctx.LogInfo <| sprintf "Resolved activation info for %O." activationRef

        return relevantActivations, relevantActiveDefinitions
    }

and activateNode (ctx: BehaviorContext<_>)
                 (activationRecord: ActivationRecord)
                 (definition: Definition)
                 (state: ClusterState)
                 (dependencyActivations: ClusterActivation [], dependencyActiveDefinitions: ClusterActiveDefinition []) = revasync {
    let! activationNodes = 
        RevAsync.FromAsync <| activationRecord.ActivationStrategy.GetActivationNodesAsync(state, activationRecord.Instance.Value, definition)

    ctx.LogInfo <| sprintf "%O :: Activating in nodes: %A" activationRecord.Definition (activationNodes |> List.map (ActorRef.toUniTcpAddress >> Option.get))

    let configuration' = definition.Configuration.Override(activationRecord.Configuration)

    if activationNodes.IsEmpty then
        ctx.LogWarning <| sprintf "%O :: Unable to find activation nodes."

    let activationRef = ActivationRecord.ToActivationReference activationRecord

    let! results = 
        activationNodes
        |> Seq.map (fun activationNode -> revasync {
            try
                ctx.LogInfo <| sprintf "%O :: Activating in node: %A..." activationRecord.Definition (ActorRef.toUniTcpAddress activationNode |> Option.get)

                //FaultPoint
                //MessageHandlingException KeyNotFoundException => definition.Path not found in node;; do not handle allow to fail;; propagate KeyNotFoundException
                //MessageHandlingException PartialActivationException => failed to properly recover an activation failure;; do not handle allow to fail;; propagate PartialActivationException
                //MessageHandlingException ActivationFailureException => failed to activate definition.Path;; do not handle allow to fail;; propagate ActivationFailureException
                //FailureException => Actor or Node level failure ;; do not handle allow to fail
                let! actorActivationResults, definitionActivationResults = 
                    NodeManager.ActivateRevAsync(ReliableActorRef.FromRef activationNode, 
                                                    activationRef, 
                                                    configuration', 
                                                    dependencyActivations, 
                                                    dependencyActiveDefinitions, 
                                                    ctx)

                ctx.LogInfo <| sprintf "%O :: Activated in node: %A" activationRecord.Definition (ActorRef.toUniTcpAddress activationNode |> Option.get)

                let newActivations = Array.toList actorActivationResults
                                    
                let newActiveDefinitions = Array.toList definitionActivationResults

                return newActivations, newActiveDefinitions
            with MessageHandlingException2 e ->
                ctx.LogWarning <| sprintf "%O :: Activation failed in node %A" activationRecord.Definition (ActorRef.toUniTcpAddress activationNode |> Option.get)
                ctx.LogError e
                ctx.LogInfo "Undoing former activations..."
                return! RevAsync.Raise e
        }) |> RevAsync.Parallel

    let clusterActivations, clusterActiveDefinitions = results |> Array.unzip
    let totalActivations = clusterActivations |> Array.toList |> List.collect id
    let totalActiveDefinitions = clusterActiveDefinitions |> Array.toList |> List.collect id

    return totalActivations, totalActiveDefinitions
}

let private activateDefinitionTree (ctx: BehaviorContext<_>) 
                                   (state: ClusterState) 
                                   (activationRecord: ActivationRecord)
                                   (externalDependencies: ClusterActivation list * ClusterActiveDefinition list) =
    revasync {
        if checkAcyclicDependencies state.DefinitionRegistry activationRecord.Definition then
            let tempState = tempUpdateStateActivations externalDependencies state
            let activationTree = computeActivationTree ctx tempState activationRecord
            return! executeActivationTree ctx tempState activationTree
        else
            //Cyclic dependencies detected 
            return! RevAsync.Raise (CyclicDefininitionDependency "Definition dependency cycle detected.")
    }


//Throws:
//ArgumentException => ActivationRecord.Instance = None
//InavalidActivationStrategy => Collocated activation strategy not supported
//CyclicDefininitionDependency => Cyclic dependency detected between dependencies of attempted activattion
//KeyNotFoundException ;; from NodeManager => definition.Path not found in node
//PartialActivationException ;; from NodeManager => failed to properly recover an activation failure
//ActivationFailureException ;; from NodeManager => failed to activate definition.Path
//FailureException;; from NodeManager
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let rec private activateDefinition (ctx: BehaviorContext<_>) 
                                   (state: ClusterState) 
                                   (activationRecord: ActivationRecord)
                                   (outerActorActivations: ClusterActivation list, outerDefinitionActivations: ClusterActiveDefinition list)
                                   (activateExternalDependencies: bool) =
    let rec activateDefinition' 
        { Definition = definitionPath; Instance = instanceId; Configuration = configuration; ActivationStrategy = activationStrategy }
        : RevAsync<ClusterActivation list * ClusterActiveDefinition list> = 
        revasync {
            if checkAcyclicDependencies state.DefinitionRegistry definitionPath then
                
                let definition = state.DefinitionRegistry.Resolve definitionPath

                //Activate non-collocated dependencies
                let externalDependencies =
                    if activateExternalDependencies then
                        ctx.LogInfo <| sprintf "%O :: Activating external dependencies..." definitionPath
                        definition.GetDependencies(configuration) 
                        |> Seq.filter (fun dep -> not dep.IsCollocated)
                        |> Seq.distinct
                        |> Seq.map (fun dep -> if dep.Instance.IsNone then { dep with Instance = instanceId } else dep)
                        |> Seq.toList
                    else []

                ctx.LogInfo <| 
                    sprintf "%O :: Dependencies %a" 
                        definitionPath 
                        DefinitionPath.ListFormatter
                        (externalDependencies |> List.map (fun dep -> dep.Definition))

                let! dependecyActivationResults =
                    externalDependencies |> Seq.map activateDefinition' |> RevAsync.Parallel

                let dependencyActivations, dependencyActiveDefinitions =
                    let a, d = dependecyActivationResults |> Array.toList |> List.unzip
                    List.collect id a, List.collect id d

                ctx.LogInfo <| sprintf "%O :: External dependencies activated." definitionPath

                //after dependencies have been sucessfully activated
                //activate this definition
                let tempState = tempUpdateStateActivations (dependencyActivations, dependencyActiveDefinitions) state
                let! activationNodes = RevAsync.FromAsync <| activationStrategy.GetActivationNodesAsync(tempState, instanceId.Value, definition)

                ctx.LogInfo <| sprintf "%O :: Activating in nodes: %A" definitionPath (activationNodes |> List.map (ActorRef.toUniTcpAddress >> Option.get))

                let configuration' = definition.Configuration.Override(configuration).Override(activationRecord.Configuration)

                if activationNodes.IsEmpty then
                    ctx.LogWarning <| sprintf "%O :: Unable to find activation nodes."

                let! results =
                    activationNodes
                    |> Seq.map (fun activationNode -> revasync {
                        let activationRef = { Definition = definitionPath; InstanceId = instanceId.Value }

                        let isActivated = 
                            outerDefinitionActivations |> List.exists (fun defAct -> defAct.ActivationReference = activationRef)
                            ||
                            ClusterActivationsDb.Create(tempState.Db.ClusterActivation, tempState.Db.ClusterActiveDefinition)
                                                //.IsActivated(activationRef)
                                                .IsActivated(activationRef, activationNode)

                        if not isActivated then
                            try
                                let dependencyActivations' =
                                    dependencyActivations@outerActorActivations 
                                    |> List.toArray

                                let dependencyActiveDefinitions' =
                                    dependencyActiveDefinitions@outerDefinitionActivations
                                    |> List.toArray

                                //FaultPoint
                                //MessageHandlingException KeyNotFoundException => definition.Path not found in node;; do not handle allow to fail;; propagate KeyNotFoundException
                                //MessageHandlingException PartialActivationException => failed to properly recover an activation failure;; do not handle allow to fail;; propagate PartialActivationException
                                //MessageHandlingException ActivationFailureException => failed to activate definition.Path;; do not handle allow to fail;; propagate ActivationFailureException
                                //FailureException => Actor or Node level failure ;; do not handle allow to fail
                                let! actorActivationResults, definitionActivationResults = 
                                    NodeManager.ActivateRevAsync(ReliableActorRef.FromRef activationNode, 
                                                                    activationRef, 
                                                                    configuration', 
                                                                    dependencyActivations', 
                                                                    dependencyActiveDefinitions', 
                                                                    ctx)

                                ctx.LogInfo <| sprintf "%O :: Activated in node: %A" definitionPath (ActorRef.toUniTcpAddress activationNode)

                                let newActivations = Array.toList actorActivationResults
                                    
                                let newActiveDefinitions = Array.toList definitionActivationResults

                                return newActivations, newActiveDefinitions
                            with MessageHandlingException2 e ->
                                ctx.LogWarning <| sprintf "%O :: Activation failed in node %A" definitionPath (ActorRef.toUniTcpAddress activationNode)
                                ctx.LogError e
                                ctx.LogInfo "Undoing former activations..."
                                return! RevAsync.Raise e
                        else
                            let existingActivations = 
                                ClusterActivation.ResolveActivation activationRef tempState.Db.ClusterActivation
                                |> Seq.toList
                            let existingActiveDefinitions = 
                                ClusterActiveDefinition.ResolveActiveDefinitions activationRef tempState.Db.ClusterActiveDefinition
                                |> Seq.toList

                            return existingActivations, existingActiveDefinitions
                    }) |> RevAsync.Parallel

                let clusterActivations, clusterActiveDefinitions = results |> Array.unzip
                let totalActivations = clusterActivations |> Array.toList |> List.collect id
                let totalActiveDefinitions = clusterActiveDefinitions |> Array.toList |> List.collect id

                return totalActivations@dependencyActivations, totalActiveDefinitions@dependencyActiveDefinitions
            else
                //Cyclic dependencies detected 
                return! RevAsync.Raise (CyclicDefininitionDependency "Definition dependency cycle detected.")
        }


    revasync {
        ctx.LogInfo (sprintf "Activating definition: %O" activationRecord.Definition)

        if activationRecord.Instance.IsNone then
            return! RevAsync.Raise <| new ArgumentException("ActivationRecord.Instance cannot be None for initial activation.")
        else
            let! activationResults = activateDefinition' activationRecord

            ctx.LogInfo (sprintf "Definition: %O activated." activationRecord.Definition)

            return activationResults
    }

//Throws
//CompensationFault => Failure in rollback ;; Compensations must be exception safe;; SYSTEM FAULT
//FinalizationFault => Failure in finalization;; Finalizations must be exception safe;; SYSTEM FAULT
//ArgumentException => ActivationRecord.Instance = None;; reversal successfull
//InvalidActivationStrategy => invalid argument ;; reversal successfull
//CyclicDefininitionDependency => invalid configuration ;; reversal successfull
//MessageHandlingException KeyNotFoundException ;; from NodeManager => definition.Path not found in node
//MessageHandlingException PartialActivationException ;; from NodeManager => failed to properly recover an activation failure;; SYSTEM FAULT
//MessageHandlingException ActivationFailureException ;; from NodeManager => failed to activate definition.Path
//SystemCorruptionException => system inconsistency;; SYSTEM FAULT
//OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private activationLoop (ctx: BehaviorContext<_>) (state: ClusterState) activationRecord externalActivations activateExternalDependencies = 
    let rec activationLoop' (allowedRetries: int) (state: ClusterState) updates = async {
        try
            //FaultPoint
            //CompensationFault => Failure in rollback ;; Compensations must be exception safe;; fall through;; SYSTEM FAULT
            //FinalizationFault => Failure in finalization;; Finalizations must be exception safe;; fall through;; SYSTEM FAULT
            //ArgumentException => ActivationRecord.Instance = None;; reversal successfull;; allow to fail
            //InvalidActivationStrategy => invalid argument ;; reversal successfull;; allow to fail
            //CyclicDefininitionDependency => invalid configuration ;; reversal successfull;; allow to fail;; exception will be replied
            //KeyNotFoundException ;; from NodeManager => definition.Path not found in node;; allow to fail;; exception will be replied
            //PartialActivationException ;; from NodeManager => failed to properly recover an activation failure;; fall through;; SYSTEM FAULT
            //ActivationFailureException ;; from NodeManager => failed to activate definition.Path;; allow to fail;; exception will be replied
            //FailureException;; from a NodeManager => node failure ;; trigger node recovery ;; retry activation
//            return! RevAsync.ToAsyncWithRecovery <| activateDefinition ctx state activationRecord externalActivations activateExternalDependencies
            return! RevAsync.ToAsyncWithRecovery <| activateDefinitionTree ctx state activationRecord externalActivations //activateExternalDependencies
        with FailureException(_, (:? ActorRef<NodeManager> as nodeManager)) as e ->
            ctx.LogWarning <| sprintf "%O :: Activation failure, will retry: %A" activationRecord.Definition e
            //FaultPoint
            //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
            //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
            let! updates' = removeNode ctx state updates nodeManager
            
            let state' = updateState state updates'

            if allowedRetries = 0 then
                return! Async.Raise <| ActivationFailureException("Maximum activation attempts limit reached.", e)

            return! activationLoop' (allowedRetries - 1) state' updates'
    }

    //TODO!!! Make this configurable
    let maxAllowedRetries = 3
    
    activationLoop' maxAllowedRetries state Seq.empty

and private deactivateDefinition (ctx: BehaviorContext<_>) (state: ClusterState) (activationRef: ActivationReference) = 
    async {
        ctx.LogInfo <| sprintf "Deactivating %O..." activationRef

        let definition = state.DefinitionRegistry.Resolve activationRef.Definition

        ctx.LogInfo <| sprintf "%O :: Deactivating external dependencies within activation tree..." activationRef

        //Deactivate non-collocated dependencies
        let externalDependencies = 
//            definition.Dependencies
//            |> Seq.filter (fun dep -> (not dep.IsCollocated) && dep.Definition.IsDescendantOf(activationRef.Definition))
//            |> Seq.distinct
//            |> Seq.map (fun dep -> if dep.Instance.IsNone then { dep with Instance = Some activationRef.InstanceId } else dep)
//            |> Seq.map (fun dep -> { Definition = dep.Definition; InstanceId = dep.Instance.Value })
//            |> Seq.toList
            getExternalDependencies2 ctx state activationRef
            |> Seq.filter (fun dep -> dep.Definition.IsDescendantOf(activationRef.Definition))
            |> Seq.map (fun dep -> { Definition = dep.Definition; InstanceId = dep.Instance.Value })
            |> Seq.toList

        ctx.LogInfo <| 
            sprintf "%O :: Dependencies %a" 
                activationRef.Definition 
                (fun () (ps: DefinitionPath list) -> 
                    let rec printlist = function [] -> "" | i::[] -> i.ToString() | i::is -> i.ToString() + "; " + (printlist is)
                    "[ " + (printlist ps) + " ]"
                )
                (externalDependencies |> List.map (fun dep -> dep.Definition))

        //Throws
        //SystemCorruptionException => a deactivation has thrown an exception
        //-
//        for externalDependency in externalDependencies do
//            do! deactivateDefinition ctx state externalDependency
        let! deactivations' =
            externalDependencies 
            |> Seq.map (deactivateDefinition ctx state)
            |> Async.Parallel

        let deactivations = deactivations' |> List.concat

        let activationsPerNode =
            //state.Db.ClusterActiveDefinition |> ClusterActiveDefinition.ResolveActiveDefinitions activationRef
            state.GetActiveDefinitionsInCluster activationRef
            |> Seq.groupBy (fun { NodeManager = nodeManager } -> nodeManager)
//            |> Seq.map (fun (nodeManager, activations) -> nodeManager.[UTCP], activations)

        //Throws
        //SystemCorruptionException => a deactivation has thrown an exception
//        for (nodeManager, activations) in activationsPerNode do
//            ctx.LogInfo <| sprintf "Deactivating %O in node %A..." activationRef (ActorRef.toUniTcpAddress nodeManager)
//
//            for activation in activations do
//                //FaultPoint
//                //MessageHandlingException => exception thrown in deactivation;; SYSTEM FAULT;; reraise to SystemCorruptionException
//                //FailureException => node failure;; do not handle;; allow to be detected later
//                try
//                    do! ReliableActorRef.FromRef nodeManager <!- fun ch -> DeActivate(ch, activation.ActivationReference, false)
//                with MessageHandlingException2 e ->
//                        return! Async.Raise <| SystemCorruptionException("Unexpected failure occurred.", e)
//                    | FailureException _ -> () //do nothing;; allow failure to be detected later

        do! activationsPerNode
            |> Seq.map (fun (nodeManager, activations) -> async {
                ctx.LogInfo <| sprintf "Deactivating %O in node %A..." activationRef (ActorRef.toUniTcpAddress nodeManager)

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

        return activationRef::deactivations
    }

and private deactivateAll (ctx: BehaviorContext<_>) (state: ClusterState) =
    async {
        ctx.LogInfo "Deactivating everything in the cluster..."
        
        let activeDefinitionRefs =
            state.GetAllActiveDefinitions()
            |> Seq.map (fun clusterActiveDefinition -> clusterActiveDefinition.ActivationReference)
            |> Seq.toList

        return! activeDefinitionRefs |> List.foldAsync (fun deactivated activationRef -> deactivateDefinition ctx state activationRef) []
    }

/// Recovers the activation of a definition of a lost node.
//Throws
//SystemCorruptionException => unexpected reactivation failure;; SYSTEM FAULT
//OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private recoverDefinitionNodeLoss 
    (ctx: BehaviorContext<_>) 
    (lostNode : ActorRef<NodeManager>) 
    (state: ClusterState) //cluster state with the lost node having been removed
    (updates: seq<ClusterStateUpdate>)
    (definition: Definition, instanceId: int, configuration: ActivationConfiguration) =

    let raiseSystemCorruption e =
        Async.Raise <| SystemCorruptionException(sprintf "System corruption during recovery of definition %O in lost node %O" definition.Path lostNode, e)

    async {
        //get dependants of definition
//        let dependantActivations = 
//            ClusterActivation.ResolveDependantsOfDefPath definition.Path state.Db.ClusterActivation
//            |> Seq.groupBy (fun activation -> activation.NodeManager)
//            |> Seq.cache
        ctx.LogInfo <| sprintf "Recovering lost %O..." definition.Path

        let dependantActivations =
            let dependantPaths =
                Cluster.DefinitionRegistry.GetDirectDependantsOf(definition.Path)
                |> Seq.map (fun def -> def.Path)
                |> Set.ofSeq

            ctx.LogInfo <| sprintf "Dependands of %O: %a" definition.Path DefinitionPath.ListFormatter (List.ofSeq dependantPaths)

            Query.from state.Db.ClusterActivation
            |> Query.where <@ fun activation -> dependantPaths |> Set.contains activation.ActivationReference.Definition @>
            |> Query.toSeq
            |> Seq.groupBy (fun activation -> activation.NodeManager)
            |> Seq.cache

        let activationRef = { Definition = definition.Path; InstanceId = instanceId }

        //Throws
        //e => unexpected exception;; reraise to SystemCorruptionException
        try
            ctx.LogInfo "Trigerring OnNodeLoss..."

            do! definition.OnNodeLoss(lostNode, activationRef)
        with e ->
            return! Async.Raise <| SystemCorruptionException("Unexpected failure in Node loss handling.", e)

        ctx.LogInfo "Triggerring NotifyDependencyLost on dependants..."

        //notify dependants of dependency loss
        do! dependantActivations
            |> Seq.map (fun (nodeManager, dependantActivations) -> async {
                let dependantActivationRefs = dependantActivations |> Seq.map (fun a -> a.ActivationReference) |> Seq.toArray
                
                try
                    //FaultPoint
                    //FailureException => node failure ;; do nothing, allow for later detectection
                    (ReliableActorRef.FromRef nodeManager) <-- NotifyDependencyLost(dependantActivationRefs, activationRef, lostNode)
                with (FailureException _) as e ->
                        ctx.LogWarning(sprintf "Failed to NotifyDependencyLost: %A" e)
            })
            |> Async.Parallel
            |> Async.Ignore

        let! updates' = async {
            //Throws
            //SystemCorruptionException => unexpected reactivation failure;; SYSTEM FAULT
            //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
            let reactivate activationStrategy = async {
                //Throws
                //SystemCorruptionException => unexpected reactivation failure;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
                try
                    //FaultPoint
                    //CompensationFault => Failure in rollback ;; Compensations must be exception safe;; allow to fail;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //FinalizationFault => Failure in finalization;; Finalizations must be exception safe;; allow to fail;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //ArgumentException => ActivationRecord.Instance = None;; reversal successfull;; invalid definition configuration;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //InvalidActivationStrategy => invalid argument ;; reversal successfull;; activation strategy should be NodeFaultReActivation;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //CyclicDefininitionDependency => invalid configuration ;; reversal successfull;; activation under recovery should not have cyclic dependencies;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //KeyNotFoundException ;; from NodeManager => definition.Path not found in node;; this is a reactivation, definition was known before;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //PartialActivationException ;; from NodeManager => failed to properly recover an activation failure;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //ActivationFailureException ;; from NodeManager => failed to activate definition.Path;; this is reactivation, it should not fail;; SYSTEM FAULT;; reraise to SystemCorruptionException
                    //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
                    //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
                    let! activationResults = 
                        activationLoop ctx state { 
                            Definition = definition.Path; 
                            Instance = Some instanceId; 
                            Configuration = configuration; 
                            ActivationStrategy = activationStrategy 
                        } ([], []) true

                    return Seq.append updates (activationStateUpdates activationResults)
                with :? System.Collections.Generic.KeyNotFoundException as e -> return! raiseSystemCorruption e
                    | ((ActivationFailureException _ | PartialActivationException _) as e) -> return! raiseSystemCorruption e
                    | SystemCorruptionException _
                    | OutOfNodesException _ as e -> return! Async.Raise e
                    | e ->  return! raiseSystemCorruption e
            }

            match definition.OnNodeLossAction lostNode with
            | ReActivateOnNodeLoss ->
                ctx.LogInfo "Reactivating lost definition..."

                let reActivationStrategy = new NodeFaultReActivation(lostNode) :> IActivationStrategy
                
                //Throws
                //SystemCorruptionException => unexpected reactivation failure;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                return! reactivate reActivationStrategy

            | ReActivateOnNodeLossSpecific activationStrategy ->
                ctx.LogInfo "Reactivating lost definition..."

                //Throws
                //SystemCorruptionException => unexpected reactivation failure;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                return! reactivate activationStrategy

            | DoNothingOnNodeLoss ->
                ctx.LogInfo "No action specified on node loss for definition."
                return updates
        }

        ctx.LogInfo "Trigerring NotifyDependencyRecovered on dependants..."

        let tempState = updateState state updates'
        let updatedDependencyActivationInfo = tempState.GetActivationInfo activationRef
        let updatedDependnecyActivations = Seq.toArray (fst updatedDependencyActivationInfo)
        let updatedDependencyActiveDefinitions = Seq.toArray (snd updatedDependencyActivationInfo)

        //notify dependants of dependency recovery
        do! dependantActivations
            |> Seq.map (fun (nodeManager, dependantActivations) -> async {
                let dependantActivationRefs = dependantActivations |> Seq.map (fun a -> a.ActivationReference) |> Seq.toArray

                try
                    //FaultPoint
                    //FailureException => node failure ;; do nothing, allow for later detectection
                    (ReliableActorRef.FromRef nodeManager) <-- NotifyDependencyRecovered(dependantActivationRefs, 
                                                                                         activationRef,
                                                                                         updatedDependnecyActivations,
                                                                                         updatedDependencyActiveDefinitions)
                with (FailureException _) as e ->
                    ctx.LogWarning(sprintf "Failed to NotifyDependencyRecovered: %A" e)
            })
            |> Async.Parallel
            |> Async.Ignore

        //Throws
        //e => unexpected exception;; reraise to SystemCorruptionException
        try
            ctx.LogInfo "Trigerring OnRecovered..."

            do! definition.OnRecovered(activationRef)
        with e ->
            return! Async.Raise <| SystemCorruptionException("Unexpected failure in OnRecovered handler.", e)

        return updates'
    }

//Throws; nothing
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and trySelectNewClusterStateLogger (clusterState: ClusterState) =
    let loggerNodes =
        Query.from clusterState.Db.ClusterAltMasterNode
        |> Query.toSeq
        |> Seq.map (fun node -> node.NodeManager)
        |> Set.ofSeq

    Query.from clusterState.Db.ClusterNode
    |> Query.where <@ fun node -> not(Set.contains node.NodeManager loggerNodes) @>
    |> Query.toSeq
    |> Seq.map (fun node -> node.NodeManager)
    |> Seq.tryHead

//Throws
//KeyNotFoundException; No available new alt master
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and selectNewClusterStateLogger (clusterState: ClusterState) =
    match trySelectNewClusterStateLogger clusterState with
    | Some alt -> alt
    | None -> raise <| new System.Collections.Generic.KeyNotFoundException("No available alt master.")

/// If the node is an AltMaster then it proceeds with AltMaster recovery
/// by selecting a new AltMaster node. If the node is not an AltMaster node then it does nothing
/// and returns the state unchanged.
//Throws
//MessageHandlingException InvalidOperationException => already a master node
//SystemCorruptionException => system inconsistency;; SYSTEM FAULT
//OutOfNodesExceptions => unalbe to recover due to lack of nodes;; SYSTEM FAULT
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private recoverAltMaster (ctx: BehaviorContext<_>) (state: ClusterState) (updates: seq<ClusterStateUpdate>) (node: ActorRef<NodeManager>) =
    let rec recoveryLoop (state: ClusterState) (updates: seq<ClusterStateUpdate>) = 
        async {
            ctx.LogInfo "Selecting new alt master..."

//            ctx.LogInfo <| sprintf "Nodes Pre: %A" (state.Db.ClusterNode.DataMap |> Map.toList |> List.map snd |> List.map (fun cn -> cn.NodeManager))
//            ctx.LogInfo <| sprintf "Dead type: %A" (node.GetType().Name)
//            ctx.LogInfo <| sprintf "Node Types: %A" (state.Db.ClusterNode.DataMap |> Map.toList |> List.map snd |> List.map (fun cn -> cn.NodeManager.GetType().Name))
//
//            let rec comparisons (n: ActorRef<NodeManager>) (ns: ActorRef<NodeManager> list) =
//                match ns with
//                | n'::ns' -> 
//                    ctx.LogInfo <| sprintf "%A comp %A = %d" n.Id n'.Id (n.CompareTo(n'))
//                    comparisons n ns'
//                | [] -> ()
//            let ns = state.Db.ClusterNode.DataMap |> Map.toList |> List.map snd |> List.map (fun cn -> cn.NodeManager)
//            ns |> List.iter (fun n -> comparisons n ns)
//
//            let clusterNodes = state.Db.ClusterNode
//            let clusterNodes' = clusterNodes |> Table.remove node
//            ctx.LogInfo <| sprintf "Table.remove nodes: %A" (clusterNodes'.DataMap |> Map.toList |> List.map snd |> List.map (fun cn -> cn.NodeManager))
//
//            let rec comparisons' (n: ActorRef<NodeManager>) (ns: ActorRef<NodeManager> list) =
//                match ns with
//                | n'::ns' -> 
//                    let nc = n :> IComparable
//                    let nc' = n' :> IComparable
//                    ctx.LogInfo <| sprintf "%A comp %A = %d" n.Id n'.Id (nc.CompareTo(nc'))
//                    comparisons' n ns'
//                | [] -> ()
//            let ns' = state.Db.ClusterNode.DataMap |> Map.toList |> List.map fst |> List.map (fun c -> c :?> ActorRef<NodeManager>)
//            ns' |> List.iter (fun n -> comparisons' n ns')
//
//            let clusterNodesMap = state.Db.ClusterNode.DataMap
//            let clusterNodesMap' = clusterNodesMap |> Map.remove (node :> IComparable)
//            ctx.LogInfo <| sprintf "Map.remove nodes: %A" (clusterNodesMap' |> Map.toList |> List.map snd |> List.map (fun cn -> cn.NodeManager))

            let state0 = state.RemoveNode node

//            ctx.LogInfo <| sprintf "Nodes Post: %A" (state0.Db.ClusterNode.DataMap |> Map.toList |> List.map snd |> List.map (fun cn -> cn.NodeManager))

            //FaultPoint
            //OutOfNodesExceptions => no more nodes;; cannot recover;; propagate;; SYSTEM FAULT
            let! newAltMasterNode = async {
                match trySelectNewClusterStateLogger state0 with
                | Some alt -> return alt
                | None -> return! Async.Raise (OutOfNodesException "Unable to recover AltMaster failure; cluster is out of nodes.")
            }

            ctx.LogInfo <| sprintf "Assuming new alt master: %A" newAltMasterNode

            //Throws
            //MessageHandlingException InvalidOperationException => already a master node;; propagate
            try
                //FaultPoint
                //MessageHandlingException InvalidOperationException => already a master node;; propagate
                //FailureException => actor (NodeManager) or node failure => node failure;; remove failed node and retry
                let! newClusterStateLogger = ReliableActorRef.FromRef newAltMasterNode <!- fun ch -> AssumeAltMaster(ch, state)

                ctx.LogInfo "Alt master ready."

                let newClusterAltMasterNode = { NodeManager = newAltMasterNode; ClusterStateLogger = newClusterStateLogger }

                let updates' = seq { yield! updates; yield ClusterRemoveAltMaster node; yield ClusterAddAltMaster newClusterAltMasterNode }
                let state' = state.RemoveAltMasterNode node
                let state'' = state'.AddAltMasterNode newClusterAltMasterNode

                ctx.LogInfo "Broadcasting new alt masters..."

                //broadcast new altmasters to cluster
                do! state0.Nodes
                    |> Seq.map ReliableActorRef.FromRef
                    |> Broadcast.post (UpdateAltMasters (state''.AltMasters |> List.toArray))
                    |> Broadcast.ignoreFaults Broadcast.allFaults
                    |> Broadcast.exec
                    |> Async.Ignore

                return state'', updates'
            with (FailureException _) as e ->
                ctx.LogWarning <| sprintf "Failed to select new alt master: %A" e
                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
                let! updates' = removeNode ctx state updates newAltMasterNode
                
                let state' = updateState state updates'
                return! recoveryLoop state' updates'
        }

    async {
        if state.AltMasterNodes |> Seq.tryFind (fun altNode -> altNode = node) |> Option.isSome then
            ctx.LogInfo "Lost node was an alt master, recovering..."
            let! state', updates' = recoveryLoop state updates
            return state'.RemoveNode node, seq { yield! updates'; yield ClusterRemoveNode node }
        else 
            return state.RemoveNode node, seq { yield! updates; yield ClusterRemoveNode node }
    }

/// Removes a node and performs any recovery operations required for that node.
//Throws
//SystemCorruptionException => system inconsistency;; SYSTEM FAULT
//OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private removeNode (ctx: BehaviorContext<_>) (state: ClusterState) (updates: seq<ClusterStateUpdate>) (node: ActorRef<NodeManager>) =
    async {
        ctx.LogInfo <| sprintf "Removing node %A..." node

        //Throws
        //SystemCorruptionException => system inconsistency;; SYSTEM FAULT;; propagate
        //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
        let! state', updates' = async {
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
            |> Seq.map (fun (activationRef, config) -> state.DefinitionRegistry.TryResolve(activationRef.Definition) 
                                                       |> Option.map (fun def -> def, activationRef.InstanceId, config))

        //all affected definitions must be in the definition registry
        assert (resolvedAffectedDefinitions |> Seq.forall Option.isSome) 

        let affectedDefinitions = 
            resolvedAffectedDefinitions 
            |> Seq.choose id
            //|> Seq.sortBy (fun (def, _, _) -> def.Path.Components)
            |> Seq.toList

        ctx.LogInfo <| sprintf "Will recover lost definitions: %a" DefinitionPath.ListFormatter (affectedDefinitions |> List.map (fun (def, _, _) -> def.Path))

        //FaultPoint
        //SystemCorruptionException => unexpected reactivation failure;; SYSTEM FAULT;; propagate
        //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT;; propagate
        return! affectedDefinitions |> List.foldAsync (recoverDefinitionNodeLoss ctx node state') updates'
    }

/// The behavior of a ClusterManager in the "proper" state. The behavior transits to the "failed" state when a system fault occurs.
//TODO!!!
//Make sure that in case of alt master node recovery, in case we run out of nodes a warning is triggered instead of a system fault
let rec private clusterManagerBehaviorProper (ctx: BehaviorContext<ClusterManager>) 
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
            do! ClusterUpdate.logClusterUpdate state' <| ClusterUpdate.FromUpdates stateUpdates

            return stay state'
        with ClusterStateLogBroadcastException(_, failedNodes) ->
                ctx.LogWarning <| sprintf "Alt master failures detected: %A" failedNodes

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

    let activateDef (ctx: BehaviorContext<_>) activateExternalDependencies replyF activationRecord externalActivations =
        async {
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                let tempState = tempUpdateStateActivations externalActivations state

                //FaultPoint
                //CompensationFault => Failure in rollback ;; Compensations must be exception safe;; SYSTEM FAULT
                //FinalizationFault => Failure in finalization;; Finalizations must be exception safe;; SYSTEM FAULT
                //InvalidActivationStrategy => invalid argument ;; reversal successfull;; reply exception
                //CyclicDefininitionDependency => invalid configuration ;; reversal successfull;; reply exception
                //KeyNotFoundException ;; from NodeManager => definition.Path not found in node;; reply exception
                //PartialActivationException ;; from NodeManager => failed to properly recover an activation failure;; SYSTEM FAULT
                //ActivationFailureException ;; from NodeManager => failed to activate definition.Path;; reply exception
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesException => unable to recover due to lack of node;; reply exception
                let! (activations, activeDefinitions) = activationLoop ctx tempState activationRecord externalActivations true
                //let! stateUpdates, (activations, activeDefinitions) = activationLoop ctx state activationRecord externalActivations

                let stateUpdates = 
                    Seq.append 
                        (activationStateUpdates externalActivations)
                        (activationStateUpdates (activations, activeDefinitions))

                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                let! transition = updateState stateUpdates

                replyF <| Value (List.toArray activations, List.toArray activeDefinitions)

                return transition
            with InvalidActivationStrategy _ 
                | CyclicDefininitionDependency _
                | OutOfNodesException _ as e ->
                    replyF <| Exception e
                    return stay state
                | :? System.Collections.Generic.KeyNotFoundException as e ->
                    replyF <| Exception e
                    return stay state
                | ActivationFailureException _ as e ->
                    replyF <| Exception e
                    return stay state
                | CompensationFault _
                | FinalizationFault _ as e ->
                    replyF <| Exception(SystemCorruptionException("Unrecoverable system failure.", e))
                    return! triggerSystemFault ctx state e
                | PartialActivationException _ as e ->
                    replyF <| Exception(SystemCorruptionException("Unrecoverable system failure.", e))
                    return! triggerSystemFault ctx state e
                | SystemCorruptionException _ as e ->
                    replyF <| Exception e
                    return! triggerSystemFault ctx state e
                | e ->
                    replyF <| Exception(SystemCorruptionException("Unrecoverable system failure.", e))
                    return! triggerSystemFault ctx state e
        }

    async {
        match msg with
        | ActivateDefinition(RR ctx reply, activationRecord) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            return! activateDef ctx true (function Value _ -> reply nothing | Exception e -> reply (Exception e)) activationRecord ([], [])

        | ActivateDefinitionWithResults(RR ctx reply, activateExternalDependencies, activationRecord, externalActivations, externalActiveDefinitions) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            return! activateDef ctx activateExternalDependencies reply activationRecord (externalActivations |> Array.toList, externalActiveDefinitions |> Array.toList)

        | DeActivateDefinition(RR ctx reply, activationRef) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                let! deactivations = deactivateDefinition ctx state activationRef

                reply nothing

                ctx.LogInfo <| sprintf "%O deactivated." activationRef

                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesException => unable to recover due to lack of node;; SYSTEM FAULT
                return! deactivations |> Seq.map ClusterDeActivateDefinition |> updateState
            with SystemCorruptionException _
                | OutOfNodesException _ as e->
                    reply <| Exception e
                    return! triggerSystemFault ctx state e
                | e ->
                    reply <| Exception(SystemCorruptionException("Unexpected failure occurred.", e))
                    return! triggerSystemFault ctx state e

        | ResolveActivationRefs(RR ctx reply, activationReference) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                let results =
                    Query.from state.Db.ClusterActivation
                    |> Query.where <@ fun clusterActivation -> clusterActivation.ActivationReference = activationReference @>
                    |> Query.toSeq
                    |> Seq.map (fun clusterActivation -> clusterActivation.ActorRef)
                    |> Seq.choose id
                    |> Seq.toArray

                reply (Value results)

                return stay state
            with e ->
                reply <| Exception(SystemCorruptionException("Unexpected failure occurred.", e))
                return! triggerSystemFault ctx state e

        | ResolveActivationInstances(RR ctx reply, definitionPath) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                Query.from state.Db.ClusterActivation
                |> Query.where <@ fun clusterActivation -> clusterActivation.ActivationReference.Definition = definitionPath @>
                |> Query.toSeq
                |> Seq.toArray
                |> Value
                |> reply

                return stay state
            with e ->
                reply <| Exception(SystemCorruptionException("Unexpected failure occurred.", e))
                return! triggerSystemFault ctx state e

        | GetAltNodes(RR ctx reply) ->
            try
                state.AltMasterNodes |> Seq.toArray |> Value |> reply

                return stay state
            with e ->
                reply <| Exception(SystemCorruptionException("Unexpected failure occurred.", e))
                return! triggerSystemFault ctx state e

        | GetAllNodes(RR ctx reply) ->
            try
                state.Nodes |> Seq.append (Seq.singleton Cluster.NodeManager)
                |> Seq.toArray
                |> Value
                |> reply

                return stay state
            with e ->
                reply <| Exception(SystemCorruptionException("Unexpected failure occurred.", e))
                return! triggerSystemFault ctx state e

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
                return! triggerSystemFault ctx state e

        | DetachNodes nodeManagers ->
            try
                //stop monitoring of nodes
                if state.HealthMonitor.IsSome then
                    for nodeAddress in nodeManagers |> Seq.map ActorRef.toUniTcpAddress |> Seq.choose id do
                        do! state.HealthMonitor.Value.AsyncPost(StopMonitoringNode nodeAddress)

                let updates = nodeManagers |> Seq.map ClusterRemoveNode

                return! updateState updates
            with e ->
                return! triggerSystemFault ctx state e

        | AddNode nodeManager ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            try
                ctx.LogInfo <| sprintf "Adding node %A to cluster..." (ActorRef.toUniTcpAddress nodeManager)

                let state' = state.AddNode nodeManager

                //attach the node to the cluster
                //FaultPoint
                //FailureException => node failure ;; do not add anything
                ReliableActorRef.FromRef nodeManager <-- AttachToCluster { Master = state.Master; AltMasters = state.AltMasters; ClusterId = state.ClusterId }

                ctx.LogInfo "Triggered AttachToCluster."

                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                return! updateState [ClusterAddNode nodeManager]
            with FailureException _ ->
                    return stay state
                | e ->                
                    return! triggerSystemFault ctx state e

        | AddNodeSync(RR ctx reply, nodeManager) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            try
                ctx.LogInfo <| sprintf "Adding node %A to cluster..." (ActorRef.toUniTcpAddress nodeManager)

                let state' = state.AddNode nodeManager

                ctx.LogInfo "Triggering AttachToCluster..."

                //attach the node to the cluster
                //FaultPoint
                //FailureException => node failure ;; do not add anything
                do! ReliableActorRef.FromRef nodeManager <!- fun ch -> AttachToClusterSync(ch, { Master = state.Master; AltMasters = state.AltMasters; ClusterId = state.ClusterId })

                reply nothing

                //FaultPoint
                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
                return! updateState [ClusterAddNode nodeManager]
            with FailureException _ ->
                    return stay state
                | e ->                
                    return! triggerSystemFault ctx state e

//        | HeartBeat nodeManager ->
//            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
//            return stay (state.UpdateHeartBeat nodeManager)
//
//        | HealthCheck ->
//            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
//            try
//                //FaultPoint
//                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
//                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
//                let! updates = 
//                    state.DeadNodes 
//                    |> Seq.toList 
//                    |> List.foldAsync (removeNode ctx state) Seq.empty 
//
//                //FaultPoint
//                //SystemCorruptionException => system inconsistency;; SYSTEM FAULT
//                //OutOfNodesExceptions => unable to recover due to lack of node;; SYSTEM FAULT
//                return! updateState updates
//            with e ->
//                return! triggerSystemFault ctx state e

        | KillCluster ->
            
            try
                ctx.LogInfo <| sprintf "KILLING CLUSTER %A..." state.ClusterId

                Cluster.NodeManager <-- StopMonitoring state.ClusterId

                //FaultPoint
                //
                let! deactivations = deactivateAll ctx state

                ctx.LogInfo <| sprintf "%A :: Triggering DetachFromCluster in Slave nodes..." state.ClusterId

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

                Cluster.NodeManager <-- DetachFromCluster

                ctx.LogInfo <| sprintf "%A :: Triggering FiniCluster in node..." state.ClusterId

                //tell the node manager to cleanup this cluster manager
                state.MasterNode <-- FiniCluster state.ClusterId

                ctx.LogInfo <| sprintf "%A :: CLUSTER KILLED" state.ClusterId

                return! deactivations |> Seq.map ClusterDeActivateDefinition |> updateState
            with e ->
                //unexpected error
                return! triggerSystemFault ctx state e

        | KillClusterSync(RR ctx reply) ->
            
            try
                ctx.LogInfo <| sprintf "KILLING CLUSTER %A..." state.ClusterId

                Cluster.NodeManager <-- StopMonitoring state.ClusterId

                let! deactivations = deactivateAll ctx state

                ctx.LogInfo <| sprintf "%A :: Triggering DetachFromCluster in Slave nodes..." state.ClusterId

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

                Cluster.NodeManager <-- DetachFromCluster

                ctx.LogInfo <| sprintf "%A :: Triggering FiniCluster in node..." state.ClusterId

                //tell the node manager to cleanup this cluster manager
                do! state.MasterNode <!- fun ch -> FiniClusterSync(ch, state.ClusterId)

                ctx.LogInfo <| sprintf "%A :: CLUSTER KILLED" state.ClusterId

                reply nothing

                state.MasterNode <-- DisposeFinalizedClusters

                return! deactivations |> Seq.map ClusterDeActivateDefinition |> updateState
            with e ->
                //unexpected error
                reply (Exception e)
                return! triggerSystemFault ctx state e

        | FailCluster e ->
            ctx.LogInfo <| sprintf "CLUSTER %A FAILURE TRIGGERED..." state.ClusterId

            return! triggerSystemFault ctx state e
    }

//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private triggerSystemFault (ctx: BehaviorContext<_>) (state: ClusterState) (e: exn) =
    async {
        ctx.LogEvent(LogLevel.Error, "SYSTEM FAULT TRIGGERED")
        ctx.LogInfo "----------------------------------"
        ctx.LogInfo(LogLevel.Error, "An unrecoverable failure occurred.")
        ctx.LogError e
        ctx.LogInfo "----------------------------------"
        ctx.LogInfo(LogLevel.Error, "Transitioning to CLUSTER FAILED STATE...")

        ctx.LogInfo "Broadcasting TriggerSystemFault to Slave nodes..."

        do! state.Nodes
            |> Seq.map ReliableActorRef.FromRef
                //FaultPoint
                //FailureException => node failure ;; do nothing
            |> Broadcast.post TriggerSystemFault
                //All failures ignored;; we are going to failed state anyway
            |> Broadcast.ignoreFaults Broadcast.allFaults
            |> Broadcast.exec
            |> Async.Ignore

        ctx.LogInfo(LogLevel.Error, "CLUSTER FAILURE.")

        return goto clusterManagerBehaviorSystemFault state
    }

and private clusterManagerBehaviorSystemFault (ctx: BehaviorContext<ClusterManager>) (state: ClusterState) (msg: ClusterManager) =
    let reply r = r <| Exception(SystemFailureException "System is in failed state.")
    let warning () = ctx.LogWarning "System is in failed state. No message is processed."
    async {
        match msg with
        | ActivateDefinition(RR ctx r, _) -> reply r
        | ActivateDefinitionWithResults(RR ctx r, _, _, _, _) -> reply r
        | DeActivateDefinition(RR ctx r, _) -> reply r
        | ResolveActivationRefs(RR ctx r, _) -> reply r
        | ResolveActivationInstances(RR ctx r, _) -> reply r
        | GetAltNodes(RR ctx r) -> reply r
        | GetAllNodes(RR ctx r) -> reply r
        | AddNodeSync(RR ctx r, _) -> reply r
        | KillClusterSync(RR ctx r) -> reply r
        | RemoveNode _
        | DetachNodes _
        | AddNode _
        | KillCluster //TODO!!! Actually kill the cluster
        | FailCluster _ -> warning()
        return stay state
    }

//Throws ;; nothing
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let createClusterManager (state: NodeState) (clusterState: ClusterState) (notifyDeadNode: ActorRef<NodeManager> -> Async<unit>) =
    let name = ("clusterManager." + clusterState.ClusterId)

    let clusterHealthMonitor =
        Actor.bind (ClusterHealthMonitor.clusterHealthMonitorBehavior notifyDeadNode ClusterHealthMonitorState.Default)
        |> Actor.subscribeLog (Default.actorEventHandler Default.fatalActorFailure String.Empty)
        |> Actor.rename ("clusterHealthMonitor." + clusterState.ClusterId)
        |> Actor.publish [Protocols.utcp()]
        |> Actor.start

    let clusterState' = { clusterState with HealthMonitor = Some clusterHealthMonitor.Ref }

    //function should be exception free
    let clusterManager =
        Actor.bind <| FSM.fsmBehavior (goto clusterManagerBehaviorProper clusterState')
        |> Actor.subscribeLog (Default.actorEventHandler Default.fatalActorFailure String.Empty)
        |> Actor.rename name
        |> Actor.publish [Protocols.utcp()]
        |> Actor.start

    let managedCluster = {
        ClusterManager = clusterManager
        AltMasters = clusterState.AltMasters
        ClusterHealthMonitor = clusterHealthMonitor
    }

    { state with ManagedClusters = state.ManagedClusters |> Map.add clusterState'.ClusterId managedCluster }

