module Nessos.Thespian.Cluster.NodeManager

open System
open Nessos.Thespian
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Remote.TcpProtocol.Unidirectional
open Nessos.Thespian.Cluster.BehaviorExtensions.FSM
open Nessos.Thespian.Utils
open Nessos.Thespian.ConcurrencyTools
open Nessos.Thespian.Reversible

type private LogLevel = Nessos.Thespian.LogLevel

//let triggerNodeEvent (ctx: BehaviorContext<_>) (handle: Async<unit>) = 
//    async {
//        try
//            do! handle
//        with e -> ctx.LogError e
//    } |> Async.Start

//Throws ;; nothing
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let private createAltMaster (state: NodeState) clusterState =
    //function should be exception free
    let clusterStateLogger = ClusterStateLogger.createClusterStateLogger clusterState

    { state with ClusterStateLogger = Some clusterStateLogger }

//Throws
//(By reply) InvalidOperationException => already a master node
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let private assumeAltMaster (ctx: BehaviorContext<_>) (state: NodeState) reply clusterState =
    async {
        if state.ClusterStateLogger.IsSome then
            let e = new InvalidOperationException(sprintf "Node %O is already an AltMaster." state.Address)
            ctx.LogError e
            reply <| Exception e
            return stay state
        else
            try
                ctx.LogInfo "Assuming alt master..."
                //Should be exception free
                let state' = createAltMaster state clusterState

                reply <| Value state'.ClusterStateLogger.Value.Ref

                return stay state'
            with e ->
                ctx.LogError e
                reply <| Exception e

                return stay state
    }

//Throws ;; nothing
let private clearState (ctx: BehaviorContext<_>) (state: NodeState) = async {
    let state' = { 
        state with 
            ActivationsMap = Map.empty 
            ClusterStateLogger = None
            ClusterInfo = None
            HeartBeat = None
    }

    try
        if state.ClusterStateLogger.IsSome then
            state.ClusterStateLogger.Value.Stop()

        if state.HeartBeat.IsSome then state.HeartBeat.Value.Dispose()

        for activation in state.ActivationsMap |> Map.toSeq |> Seq.map snd do
            do! activation.DeactivateAsync()
    with e -> ctx.LogWarning e
        
    Cluster.SetClusterManager None

    return state'
}

//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
let rec private initCluster (ctx: BehaviorContext<_>) 
                            (state: NodeState) 
                            (clusterConfiguration: ClusterConfiguration) 
                            reply =
    let initClusterProper() =
        async {
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                ctx.LogInfo "CLUSTER INIT..."
                ctx.LogInfo "---------------"
                ctx.LogInfo <| sprintf "Cluster id: %A" clusterConfiguration.ClusterId
                ctx.LogInfo <| sprintf "Master node: %A" Cluster.NodeManager
                ctx.LogInfo <| sprintf "Slave nodes: %A" clusterConfiguration.Nodes

                ctx.LogInfo "Initializing cluster state..."

                //create initial cluster state
                //TODO!!! Make time parameters configurable
                let self = state.NodeManager
                let initClusterState = ClusterState.New(clusterConfiguration.ClusterId, self, TimeSpan.FromSeconds(15.0), state.DefinitionRegistry)

                //add nodes to state
                let clusterState =
                    clusterConfiguration.Nodes |> Seq.fold (fun (clusterState: ClusterState) node -> clusterState.AddNode node) initClusterState

                ctx.LogInfo "Selecting cluster alt master nodes..."

                //select alt masters
                let altMasters = clusterConfiguration.Nodes |> Seq.truncate clusterConfiguration.FailoverFactor

                ctx.LogInfo <| sprintf "Selected alt masters: %A" (altMasters |> Seq.toList)

                //TODO!!! TRIGGER WARNING IF alt masters less than failover factor

                ctx.LogInfo "Initializing cluster alt master nodes..."

                //create alt masters
                let! clusterAltMasterNodes =
                    altMasters
                    |> Broadcast.action (fun node -> async {
                            try
                                //FaultPoint
                                //InvalidOperationException => already a master node ;; do nothing, ignore this node
                                //FailureException => node failure ;; do nothing, ignore this node
                                let! clusterStateLogger = node <!- fun ch -> AssumeAltMaster(ch, clusterState)
                                return { NodeManager = node; ClusterStateLogger = clusterStateLogger }
                            with e ->
                                ctx.LogError e
                                return! Async.Raise e
                        })
                    //All failures are ignored
                    |> Broadcast.ignoreFaults Broadcast.allFaults
                    |> Broadcast.exec

                ctx.LogInfo <| sprintf "Cluster alt master nodes initialized."

                //TODO!!! Trigger warning if less alt masters activated than failover factor (if failures occurred)

                //cluster state with alt masters added
                let clusterStateUpdate = 
                    clusterAltMasterNodes |> Seq.map ClusterAddAltMaster
                    |> ClusterUpdate.FromUpdates

                let clusterState' = ClusterUpdate.Update(clusterState, clusterStateUpdate)

//                ctx.LogInfo <| sprintf "Cluster alt masters: %A" clusterState'.AltMasters
//                ctx.LogInfo <| sprintf "Cluster alt master nodes: %A" (clusterState'.AltMasterNodes |> Seq.toList)

                assert ((clusterState'.AltMasterNodes |> Seq.toList) = (altMasters |> Seq.toList))

                //update alt masters and create cluster state manager
                let! failures = async {
                    try
                        //Throws
                        //ClusterStateLogBroadcastException => some updates have failed (node failures) ;; tell cluster manager of failures
                        //ClusterStateLogFailureException => total failure to update (all alt master node failures) ;; trigger MASSIVE WARNING
                        do! ClusterUpdate.logClusterUpdate clusterState' clusterStateUpdate

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
                let state' = ClusterManager.createClusterManager state clusterState'' clusterConfiguration.NodeDeadNotify

                ctx.LogInfo "ClusterManager started."

                if not totalAltMasterFailure then
                    for failedNode in failures do
                        //In memory communication
                        state'.ManagedClusters.[clusterState.ClusterId].ClusterManager.Ref <-- RemoveNode failedNode

                let clusterState''' =
                    failures |> Seq.fold (fun (clusterState: ClusterState) failedNode -> clusterState.RemoveNode failedNode) clusterState''

                //reply the addresses of alt master nodes
                if totalAltMasterFailure then
                    reply <| Value Array.empty
                else
                    clusterState'''.AltMasters |> List.toArray |> Value |> reply

                ctx.LogInfo "Attaching slave nodes to cluster..."

                let clusterInfo = {
                    Master = state.Address
                    AltMasters = clusterState'''.AltMasters
                    ClusterId = clusterConfiguration.ClusterId
                }

                let slaves = 
                    let initial = clusterConfiguration.Nodes |> Set.ofArray
                    let failed = failures |> Set.ofList
                    Set.difference initial failed
            
                do! slaves
                    |> Broadcast.postWithReply (fun ch -> AttachToClusterSync(ch, clusterInfo))
                    //|> Broadcast.post (AttachToCluster clusterInfo)
                    |> Broadcast.ignoreFaults Broadcast.allFaults
                    |> Broadcast.onFault (fun (failedNode, _) ->
                        state'.ManagedClusters.[clusterState.ClusterId].ClusterManager.Ref <-- RemoveNode failedNode
                    )
                    |> Broadcast.exec
                    |> Async.Ignore
                //OnAddToCluster event is guaranteed to be handled in each node after this point

                ctx.LogInfo "Attaching master node to cluster..."
            
                if state.ClusterInfo.IsNone then Cluster.SetClusterManager <| Some(ReliableActorRef.FromRef <| state'.ManagedClusters.["HEAD"].ClusterManager.Ref)
            
                ctx.LogInfo "Triggering OnAddToCluster..."
            
                state.NodeEventExecutor.Ref <-- 
                    ExecEvent(
                        ctx,
                        state'.EventManager.OnAddToCluster(clusterConfiguration.ClusterId, state'.ManagedClusters.[clusterConfiguration.ClusterId].ClusterManager.Ref |> ReliableActorRef.FromRef)
                    )

                ctx.LogInfo "Triggering OnClusterInit..."
            
                state.NodeEventExecutor.Ref <-- 
                    ExecEvent(
                        ctx,
                        state.EventManager.OnClusterInit(clusterConfiguration.ClusterId, state'.ManagedClusters.[clusterConfiguration.ClusterId].ClusterManager.Ref)
                    )

                state.NodeEventExecutor.Ref <--
                    ExecEvent(
                        ctx,
                        state.EventManager.OnMaster(clusterConfiguration.ClusterId)
                    )

                ctx.LogInfo "CLUSTER INIT COMPLETED."

                if state.ManagedClusters.Count = 0 then
                    //Then there were no managed clusters,
                    //so this is the first one
                    return goto nodeManagerProper state'
                else
                    //This is not the first managed cluster
                    //so stay in whatever state we're already in
                    return stay state'
            with e ->
                //unexpected exception occurred ;; goto failed state
                reply (Exception <| SystemCorruptionException("Unexpected exception in cluster initialization.", e))
                
                return! gotoNodeSystemFault ctx state e
        }

    async {
        if not <| state.ManagedClusters.ContainsKey clusterConfiguration.ClusterId then
            return! initClusterProper()
        else
            ctx.LogWarning <| sprintf "Managed cluster with id %A already initialized." clusterConfiguration.ClusterId
            state.ManagedClusters.[clusterConfiguration.ClusterId].AltMasters |> List.toArray |> Value |> reply
            return stay state
    }

//Throws ;; nothing
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private resolve (ctx: BehaviorContext<_>) reply state activationRef =
    async {
        try
            //Throws
            //ActivationResolutionException => activationRef not found;; reply to client
            NodeRegistry.Registry.ResolveLocal activationRef
            |> Value
            |> reply

            return stay state
        with ActivationResolutionException _ as e ->
                reply <| Exception (new System.Collections.Generic.KeyNotFoundException("Activation reference not found in this node.", e))

                return stay state
            | e ->
                reply <| Exception(SystemCorruptionException(sprintf "An unpected error occured while trying to resolve %A." activationRef, e))
                return! gotoNodeSystemFault ctx state e
    }

and private tryResolve (ctx: BehaviorContext<_>) reply state activationRef =
    async {
        try
            NodeRegistry.Registry.TryResolveLocal activationRef
            |> Value
            |> reply

            return stay state
        with e ->
            reply <| Exception(SystemCorruptionException(sprintf "An unpected error occured while trying to resolve %A." activationRef, e))
            return! gotoNodeSystemFault ctx state e
    }

//Throws ;; nothing
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private tryGetManagedCluster (ctx: BehaviorContext<_>) reply state clusterId =
    async {
        try
            state.ManagedClusters |> Map.tryFind clusterId
            |> Option.map (fun managedCluster -> managedCluster.ClusterManager.Ref)
            |> Value
            |> reply
        with e -> ctx.LogError e

        return stay state
    }

//Throws
//KeyNotFoundException => unknown cluster
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private finiCluster (ctx: BehaviorContext<_>) (state: NodeState) (clusterId: ClusterId) =
    async {
        ctx.LogInfo <| sprintf "FINI CLUSTER %A..." clusterId

        //Throws
        //KeyNotFoundException => unknown cluster
        let managedCluster = state.ManagedClusters.[clusterId]

        ctx.LogInfo <| sprintf "%A :: Stopping clusterManager services..." clusterId

        (managedCluster.ClusterHealthMonitor :> IDisposable).Dispose()
        managedCluster.ClusterManager.Stop()

        let state' = { state with ManagedClusters = state.ManagedClusters |> Map.remove clusterId }

        ctx.LogInfo <| sprintf "%A :: Stopped..." clusterId

//        if clusterId = "HEAD" then
//            for managedCluster in state.ManagedClusters |> Map.toSeq |> Seq.filter (fun (clusterId', _) -> clusterId' <> clusterId) |> Seq.map snd do
//                !managedCluster.ClusterManager <-- KillCluster

        ctx.LogInfo "CLUSTER FINILIZED."

        return state'
    }

and private finiClusterSync (ctx: BehaviorContext<_>) (state: NodeState) (clusterId: ClusterId) =
    async {
        ctx.LogInfo <| sprintf "FINI CLUSTER %A..." clusterId

        //Throws
        //KeyNotFoundException => unknown cluster
        let managedCluster = state.ManagedClusters.[clusterId]

        ctx.LogInfo <| sprintf "%A :: Stopping clusterManager services..." clusterId

        (managedCluster.ClusterHealthMonitor :> IDisposable).Dispose()

        let state' = { 
            state with 
                ManagedClusters = state.ManagedClusters |> Map.remove clusterId 
                FinilizedClusterManagers = (managedCluster.ClusterManager :> IDisposable)::state.FinilizedClusterManagers
        }

        ctx.LogInfo <| sprintf "%A :: Stopped..." clusterId

//        if clusterId = "HEAD" then
//            for managedCluster in state.ManagedClusters |> Map.toSeq |> Seq.filter (fun (clusterId', _) -> clusterId' <> clusterId) |> Seq.map snd do
//                !managedCluster.ClusterManager <-- KillCluster

        ctx.LogInfo "CLUSTER FINILIZED."

        return state'
    }

and private disposeClusters (state: NodeState) =
    for disposable in state.FinilizedClusterManagers do disposable.Dispose()
    { state with FinilizedClusterManagers = [] }

and private attachToCluster (ctx: BehaviorContext<_>) (state: NodeState) clusterInfo = 
    async {
        let state' = { state with ClusterInfo = Some clusterInfo }

        ctx.LogInfo <| sprintf "Attaching node to cluster: %A" clusterInfo

        Cluster.SetClusterManager(Some state'.ClusterInfo.Value.ClusterManager)

        let state'' =
            if state.Address <> clusterInfo.Master then

                let healthMonitor = 
                    let serializer = Serialization.SerializerRegistry.GetDefaultSerializer().Name
                    ActorRef.fromUri (sprintf "utcp://%A/*/clusterHealthMonitor.%s/%s" clusterInfo.Master clusterInfo.ClusterId serializer)
                    |> ReliableActorRef.FromRef

                let nodeHeartBeat = 
                    Actor.bind (NodeHeartBeat.nodeHeartBeatBehavior healthMonitor)
                    |> Actor.rename "nodeHeartBeat"
                    |> Actor.subscribeLog (Default.actorEventHandler Default.fatalActorFailure String.Empty)
                    |> Actor.publish [UTcp()]
                    |> Actor.start

                try
                    //FaultPoint
                    //FailureException => masterNode failure;; trigger master node loss
                    healthMonitor <-- StartMonitoringNode state.Address
                with FailureException _ ->
                    ctx.Self <-- MasterNodeLoss

                { state' with HeartBeat = Some(nodeHeartBeat :> IDisposable) }
            else state'

        ctx.LogInfo "Triggering OnAddToCluster..."
        
        state''.NodeEventExecutor.Ref <--
            ExecEvent(
                ctx,
                state''.EventManager.OnAddToCluster(clusterInfo.ClusterId, clusterInfo.ClusterManager)
            )

        ctx.LogInfo "Attach to cluster complete."

        return state''
    }

//Throws
//SystemCorruptionException => SYSTEM FAULT
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private getNodeType (ctx: BehaviorContext<_>) reply (state: NodeState) =
    async {
        try
            let nodeType =
                if state.ClusterInfo.IsNone && not(state.ManagedClusters.ContainsKey "HEAD") then NodeType.Idle
                else if state.ClusterInfo.IsNone then NodeType.Master
                else NodeType.Slave

            reply (Value nodeType)

            return stay state
        with e ->
            reply <| Exception(SystemCorruptionException("Unexpected failure occurred.", e))
            return! gotoNodeSystemFault ctx state e
    }

//Throws ;; nothing
and private gotoNodeSystemFault (ctx: BehaviorContext<_>) (state: NodeState) (e: exn) = 
    async {
        ctx.LogEvent(LogLevel.Error, "SYSTEM FAULT TRIGGERED")
        ctx.LogInfo "----------------------------------"
        ctx.LogInfo(LogLevel.Error, "An unrecoverable failure occurred.")
        ctx.LogError e
        ctx.LogInfo "----------------------------------"
        ctx.LogInfo(LogLevel.Error, "Transitioning to NODE FAILED STATE...")

        let! state' = clearState ctx state

        ctx.LogInfo "Node state cleared."

        try
            ctx.LogInfo "Triggering OnSystemFault..."
            do! state.EventManager.OnSystemFault()
        with e -> ctx.LogWarning e

        ctx.LogInfo(LogLevel.Error, "NODE HAS FAILED.")

        return goto nodeManagerSystemFault state'
    }

and private syncNodeEvents (ctx: BehaviorContext<_>) reply (state: NodeState) =
    async {
        //offload event syncing and reply
        async {
            try
                do! !state.NodeEventExecutor <!- ExecSync
                reply nothing
            with e ->
                ctx.LogError e
                reply (Exception e)
        } |> Async.Start

        return stay state
    }

/// The behavior of a NodeManager in the "initialized" state. This is when the node is either not attached to a cluster,
/// or when the node has been notified that the master node of the cluster has been lost.
and private nodeManagerInit (ctx: BehaviorContext<NodeManager>) (state: NodeState) (msg: NodeManager) =
    async {
        match msg with
        | InitCluster(RR ctx reply, clusterConfiguration) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            return! initCluster ctx state clusterConfiguration reply

        | StopMonitoring clusterId ->
            try
                ctx.LogInfo <| sprintf "Stopping cluster %A monitoring..." clusterId

                match state.ManagedClusters.TryFind clusterId with
                | Some managedCluster ->
                    !managedCluster.ClusterHealthMonitor <-- DisableMonitoring
                | _ ->
                    ctx.LogWarning <| sprintf "Cluster %A is unknown" clusterId

                return stay state
            with e ->
                //unexpected error
                return! gotoNodeSystemFault ctx state e

        | StartMonitoring clusterId ->
            try
                ctx.LogInfo <| sprintf "Starting cluster %A monitoring..." clusterId

                match state.ManagedClusters.TryFind clusterId with
                | Some managedCluster ->
                    !managedCluster.ClusterHealthMonitor <-- EnableMonitoring
                | _ ->
                    ctx.LogWarning <| sprintf "Cluster %A is unknown" clusterId

                return stay state
            with e ->
                //unexpected error
                return! gotoNodeSystemFault ctx state e

        | FiniCluster clusterId ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //Throws
                //KeyNotFoundException => unknown cluster ;; trigger warning
                let! state' = finiCluster ctx state clusterId

                do! !state'.NodeEventExecutor <!- ExecSync

                return stay state'
            with e ->
                //unexpected error
                return! gotoNodeSystemFault ctx state e

        | FiniClusterSync(RR ctx reply, clusterId) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //Throws
                //KeyNotFoundException => unknown cluster ;; trigger warning
                let! state' = finiClusterSync ctx state clusterId

                do! !state'.NodeEventExecutor <!- ExecSync

                reply nothing

                return stay state'
            with e ->
                //unexpected error
                reply (Exception e)
                return! gotoNodeSystemFault ctx state e

        | DisposeFinilizedClusters ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                let state' = disposeClusters state

                return stay state'
            with e ->
                //unexpected error
                return! gotoNodeSystemFault ctx state e

        | Activate(RR ctx reply, _, _, _, _) ->
            reply <| Exception(NodeNotInClusterException <| sprintf "Node %O is not part of an active cluster." state.Address)

            return stay state

        | DeActivate(RR ctx reply, _, _) ->
            reply <| Exception(NodeNotInClusterException <| sprintf "Node %O is not part of an active cluster." state.Address)

            return stay state

        | Resolve(RR ctx reply, activationRef) ->
            return! resolve ctx reply state activationRef

        | TryResolve(RR ctx reply, activationRef) ->
            return! tryResolve ctx reply state activationRef

        | TryGetManagedCluster(RR ctx reply, clusterId) ->
            return! tryGetManagedCluster ctx reply state clusterId

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
                let! state' = attachToCluster ctx state clusterInfo

                ctx.LogInfo "Triggering OnAttachToCluster..."

                state'.NodeEventExecutor.Ref <--
                    ExecEvent(
                        ctx,
                        state'.EventManager.OnAttachToCluster(clusterInfo.ClusterId, clusterInfo.ClusterManager)
                    )

                return goto nodeManagerProper state'
            with e -> 
                ctx.LogWarning e

                return stay state

        | AttachToClusterSync(RR ctx reply, clusterInfo) ->
            try
                let! state' = attachToCluster ctx state clusterInfo
                
                //offload reply to other thread
                state'.NodeEventExecutor.Ref <-- ExecEvent(ctx, async { reply nothing })

                return goto nodeManagerProper state'
            with e ->
                ctx.LogWarning e
                reply (Exception e)
                return stay state

        | DetachFromCluster ->
            //In this state this message is invalid
            ctx.LogWarning(sprintf "Unable to process %A in current node state." msg)

            return stay state

        | AssumeAltMaster(RR ctx reply, clusterState) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            return! assumeAltMaster ctx state reply clusterState

        | UpdateAltMasters _ ->
            //In this state this message is invalid
            ctx.LogWarning(sprintf "Unable to process %A in current node state." msg)

            return stay state

        | TriggerSystemFault ->
            return! gotoNodeSystemFault ctx state (SystemFailureException "Node received TriggerSystemFault.")

        | GetNodeType(RR ctx reply) ->
            return! getNodeType ctx reply state

        | SyncNodeEvents(RR ctx reply) ->
            return! syncNodeEvents ctx reply state
    }

//Throws
//KeyNotFoundException => argument error
//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and private activateDefinition (ctx: BehaviorContext<_>) 
                               (state: NodeState) 
                               (activationRef: ActivationReference) 
                               (configuration: ActivationConfiguration)
                               (externalActorActivations: ClusterActivation [], externalDefinitionActivations: ClusterActiveDefinition []) =

    let rec activateDefinition' (definitionPath: DefinitionPath, instanceId: int, configuration: ActivationConfiguration) = 
        revasync {
            //Throws
            //KeyNotFoundException => argument error ;; allow to fail, undoes will be performed
            let definition = state.DefinitionRegistry.Resolve definitionPath

            ctx.LogInfo (sprintf "%O :: Definition resolved." definitionPath)

            let dependencies = definition.GetDependencies(configuration)

            let collocatedDependencies =
                definition.GetDependencies(configuration) 
                //remove non-collocated deps
                |> Seq.filter (fun dep -> dep.IsCollocated)
                //remove deps already activated; 
                //NOTE is this correct? This is checked against ClusterActivations. Maybe it should be checked against ClusterActiveDefinitions.
                |> Seq.filter (fun dep -> Cluster.NodeRegistry.TryResolveLocal { Definition = dep.Definition; InstanceId = match dep.Instance with Some iid -> iid | _ -> instanceId } |> Option.isNone)
                |> Seq.distinct
                |> Seq.map (fun dep -> dep.Definition, (if dep.Instance.IsNone then instanceId else dep.Instance.Value), dep.Configuration.Override(configuration))
                |> Seq.toList

            ctx.LogInfo <| sprintf "%O :: Activating collocated dependencies: %a" definitionPath DefinitionPath.ListFormatter (collocatedDependencies |> List.map (fun (p, _, _) -> p))

            let! dependencyResults = collocatedDependencies |> List.mapRevAsync activateDefinition'
            
            let dependencyActivations = List.collect id dependencyResults
            let actorActivations = 
                dependencyActivations
                |> Seq.map (fun (activation: Activation) -> activation.ActorActivationResults)
                |> Seq.collect id
                |> Seq.toArray
                |> Array.append externalActorActivations
            let definitionActivations =
                dependencyActivations
                |> Seq.map (fun (activation: Activation) -> activation.DefinitionActivationResults)
                |> Seq.collect id
                |> Seq.toArray
                |> Array.append externalDefinitionActivations

            ctx.LogInfo (sprintf "%O :: Collocated dependencies activated." definitionPath)

            let activateF, isInternal =
                match state.ActivationPatterns |> Seq.map (fun p ->
                                    match state.ManagedClusters.TryFind p.ClusterId with
                                    | Some { ClusterManager = clusterManager } -> p.PatternF ctx !clusterManager { Definition = definitionPath; InstanceId = instanceId } (actorActivations, definitionActivations)
                                    | None -> None)
                      |> Seq.tryFind Option.isSome with
                | Some(Some activateF) -> activateF, false
                | Some None
                | None -> definition.ActivateAsync, true

            ctx.LogInfo (sprintf "%O :: Activating..." definitionPath)

            let! activation = activateF(instanceId, configuration)

            if isInternal then
                ctx.LogInfo (sprintf "%O :: Activated. Triggering OnActivated..." definitionPath)
                do! definition.OnActivated activation

            return activation::dependencyActivations
//            //Throws
//            //KeyNotFoundException => argument error ;; allow to fail, undoes will be performed
//            let definition = state.DefinitionRegistry.Resolve definitionPath
//
//            ctx.LogInfo (sprintf "%O :: Definition resolved." definitionPath)
//
//            let activateF, isInternal =
//                match state.ActivationPatterns |> Seq.map (fun p ->
//                                    match state.ManagedClusters.TryFind p.ClusterId with
//                                    | Some { ClusterManager = clusterManager } -> p.PatternF ctx !clusterManager { Definition = definitionPath; InstanceId = instanceId } (actorActivations, definitionActivations)
//                                    | None -> None)
//                      |> Seq.tryFind Option.isSome with
//                | Some(Some activateF) -> activateF, false
//                | Some None
//                | None -> definition.ActivateAsync, true
//
//            if isInternal then
//                let dependencies = definition.GetDependencies(configuration)
//
//                let collocatedDependencies =
//                    definition.GetDependencies(configuration) 
//                    //remove non-collocated deps
//                    |> Seq.filter (fun dep -> dep.IsCollocated)
//                    //remove deps already activated; 
//                    //NOTE is this correct? This is checked against ClusterActivations. Maybe it should be checked against ClusterActiveDefinitions.
//                    |> Seq.filter (fun dep -> Cluster.NodeRegistry.TryResolveLocal { Definition = dep.Definition; InstanceId = match dep.Instance with Some iid -> iid | _ -> instanceId } |> Option.isNone)
//                    |> Seq.distinct
//                    |> Seq.map (fun dep -> dep.Definition, (if dep.Instance.IsNone then instanceId else dep.Instance.Value), dep.Configuration.Override(configuration))
//                    |> Seq.toList
//
//                ctx.LogInfo <| sprintf "%O :: Activating collocated dependencies: %a" definitionPath DefinitionPath.ListFormatter (collocatedDependencies |> List.map (fun (p, _, _) -> p))
//
//                let! dependencyResults = collocatedDependencies |> List.mapRevAsync activateDefinition'
//            
//                let dependencyActivations = List.collect id dependencyResults
//                let actorActivations = 
//                    dependencyActivations
//                    |> Seq.map (fun (activation: Activation) -> activation.ActorActivationResults)
//                    |> Seq.collect id
//                    |> Seq.toArray
//                    |> Array.append externalActorActivations
//                let definitionActivations =
//                    dependencyActivations
//                    |> Seq.map (fun (activation: Activation) -> activation.DefinitionActivationResults)
//                    |> Seq.collect id
//                    |> Seq.toArray
//                    |> Array.append externalDefinitionActivations
//
//                ctx.LogInfo (sprintf "%O :: Collocated dependencies activated." definitionPath)
//
//                ctx.LogInfo (sprintf "%O :: Activating..." definitionPath)
//
//                let! activation = activateF(instanceId, configuration)
//
//                if isInternal then
//                    ctx.LogInfo (sprintf "%O :: Activated. Triggering OnActivated..." definitionPath)
//                    do! definition.OnActivated activation
//
//                return activation::dependencyActivations
//            else
//                ctx.LogInfo (sprintf "%O :: Activating..." definitionPath)
//
//                let! activation = activateF(instanceId, configuration)
//
//                return activation::[]
        }

    activateDefinition' (activationRef.Definition, activationRef.InstanceId, configuration)

and private deactivateDefinition (ctx: BehaviorContext<_>) 
                                 (state: NodeState) 
                                 (activationRef: ActivationReference) 
                                 (throwOnNotExisting: bool) = 
    async {
        match state.ActivationsMap.TryFind activationRef with
        | Some activation ->
            ctx.LogInfo "Triggering OnDeactivate..."

            do! activation.Definition.OnDeactivate activation

            let definition = state.DefinitionRegistry.Resolve activationRef.Definition
            let collocatedDependencies =
                definition.Dependencies
                |> Seq.filter (fun dep -> dep.IsCollocated)
                |> Seq.distinct
                |> Seq.map (fun dep -> { Definition = dep.Definition; InstanceId = dep.Instance |> Option.bind2 id activationRef.InstanceId })
                |> Seq.toList

            ctx.LogInfo <| sprintf "Deactivating collocated dependencies of %O: %a" activationRef DefinitionPath.ListFormatter (collocatedDependencies |> List.map (fun d -> d.Definition))

            let! state' = collocatedDependencies |> List.foldAsync (fun s d -> deactivateDefinition ctx s d throwOnNotExisting) state

            ctx.LogInfo <| sprintf "Collocated dependencies of %O deactivated." activationRef

            //Throws
            //e => Failed to deactivate;; allow to fail;; exception will be replied
            do! activation.DeactivateAsync()

            Cluster.NodeRegistry.UnRegisterDescendants activationRef

            ctx.LogInfo <| sprintf "Activation %O deactivated." activationRef

            return { state' with ActivationsMap = state.ActivationsMap |> Map.remove activationRef }
        | None ->
            ctx.LogWarning <| sprintf "Activation %O not found!" activationRef

            if throwOnNotExisting then
                return! Async.Raise <| new System.Collections.Generic.KeyNotFoundException(sprintf "No activation found by activation reference: %A" activationRef)
            else return state
    }

///The behavior of a NodeManager in the "normal" state.
and private nodeManagerProper (ctx: BehaviorContext<NodeManager>) (state: NodeState) (msg: NodeManager) =
    async {
        match msg with
        | Activate(RR ctx reply, activationRef, configuration, dependencyActivations, dependencyActiveDefinitions) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                let isActivated = Cluster.NodeRegistry.IsActivatedLocally activationRef

                if not isActivated then

                    ctx.LogInfo (sprintf "Activating %O" activationRef)
                
                    state.NodeRegistry.RegisterBatchOverriding(dependencyActivations, dependencyActiveDefinitions)

                    ctx.LogInfo (sprintf "%O :: External dependencies registered." activationRef)

                    //Throws
                    //PartialActivationException => allow to fall through;; will be replied
                    //ActivationFailureException => allow to fall through;; will be replied
                    let! activations = async {
                        try
                            //Throws
                            //CompensationFault => activation failed; recovery of activation failed;; reraise to PartialActivationException
                            //FinalizationFault => activation failed; recovery successfull;; finalization failed;; reraise to PartialActivationException
                            //KeyNotFoundException => activation failed; recovery sucessfull; reraise to ActivationFailureException
                            return! RevAsync.ToAsyncWithRecovery <| activateDefinition ctx state activationRef configuration (dependencyActivations, dependencyActiveDefinitions)
                        with CompensationFault e 
                             | FinalizationFault e -> return! Async.Raise <| PartialActivationException((sprintf "Activation of %O failed. Recovery of failure failed." activationRef), e)
                             | e -> return! Async.Raise <| ActivationFailureException((sprintf "Activation of %O failed." activationRef), e)
                    }
            
                    let actorActivationResults =
                        activations 
                        |> Seq.collect (fun activation -> activation.ActorActivationResults)
                        |> Seq.toArray

                    let definitionActivationResults =
                        activations
                        |> Seq.collect (fun activation -> activation.DefinitionActivationResults)
                        |> Seq.toArray
                
                    reply <| Value (actorActivationResults, definitionActivationResults)

                    ctx.LogInfo (sprintf "%O :: activation completed." activationRef.Definition)

                    return stay { 
                        state with 
                            ActivationsMap = activations |> Seq.fold (fun activationsMap activation -> 
                                activationsMap |> Map.add activation.ActivationReference activation) state.ActivationsMap
                    }
                
                else
                    ctx.LogInfo (sprintf "%O is already activated in current node." activationRef)

                    let activations, activeDefinitions = Cluster.NodeRegistry.ResolveLocalActivationData activationRef

                    (Seq.toArray activations, 
                     Seq.toArray activeDefinitions)
                    |> Value
                    |> reply

                    return stay state

            with (ActivationFailureException _ | PartialActivationException _) as e ->
                    ctx.LogWarning (sprintf "Failed to activate %O" activationRef.Definition)
                    ctx.LogError e
                    reply (Exception e)
                    return stay state
                | e -> 
                    ctx.LogWarning (sprintf "Unexpected failure while activating %O" activationRef.Definition)
                    reply (Exception e)
                    return stay state

        | DeActivate(RR ctx reply, activationRef, throwOnNotExisting) ->
            try
                let! state' = deactivateDefinition ctx state activationRef throwOnNotExisting

                reply nothing

                return stay state'
            with e ->
                reply (Exception e)
                return stay state

        | Resolve(RR ctx reply, activationRef) ->
            return! resolve ctx reply state activationRef

        | TryResolve(RR ctx reply, activationRef) ->
            return! tryResolve ctx reply state activationRef

        | TryGetManagedCluster(RR ctx reply, clusterId) ->
            return! tryGetManagedCluster ctx reply state clusterId

        | NotifyDependencyLost(dependants, dependency, lostNode) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            try
                for dependant in dependants do
                    try
                        //Throws
                        //KeyNotFoundException => dependant does not exist in this node
                        let dependantDefinition = state.DefinitionRegistry.Resolve dependant.Definition

                        //Throws
                        //KeyNotFoundException => dependant is not activated in this node
                        //let dependantActivation = state.ActivationsMap.[dependant]
                        let dependantActivation = state.ResolveActivation dependant

                        ctx.LogInfo <| sprintf "Triggering OnDependencyLoss for dependency %O on dependant %O" dependency dependant
                        do! dependantDefinition.OnDependencyLoss(dependantActivation, dependency, lostNode)
                    with :? System.Collections.Generic.KeyNotFoundException as e ->
                        ctx.LogWarning e
                        ctx.LogInfo <| sprintf "Dependant not found: %O" dependant

                ctx.LogInfo <| sprintf "Removing lost dependency %O from NodeRegistry" dependency
                state.NodeRegistry.UnRegisterActivation(lostNode, dependency)

                return stay state
            with e ->
                //unexpected exception
                return! gotoNodeSystemFault ctx state e

        | NotifyDependencyRecovered(dependants, dependency, updatedDependencyActivations, updatedDependenyActiveDefinitions) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            try
                //TODO!!! Also make sure that the dependant is indeed activated on this node

                ctx.LogInfo <| sprintf "Updating NodeRegistry with activations %a..." DefinitionPath.ListFormatter (updatedDependencyActivations |> Array.toList |> List.map (fun uda -> uda.ActivationReference.Definition))

                state.NodeRegistry.RegisterBatchOverriding(updatedDependencyActivations, updatedDependenyActiveDefinitions)

                for dependant in dependants do
                    try
                        //Throws
                        //KeyNotFoundException => dependant does not exist in this node
                        let dependantDefinition = state.DefinitionRegistry.Resolve dependant.Definition

                        //Throws
                        //KeyNotFoundException => dependant is not activated in this node
                        //let dependantActivation = state.ActivationsMap.[dependant]
                        let dependantActivation = state.ResolveActivation dependant

                        ctx.LogInfo <| sprintf "Triggering OnDependencyLossRecovery for dependency %O on dependant %O" dependency dependant
                        do! dependantDefinition.OnDependencyLossRecovery(dependantActivation, dependency)
                    with :? System.Collections.Generic.KeyNotFoundException as e ->
                        ctx.LogWarning e
                        ctx.LogInfo <| sprintf "Dependant not found: %O" dependant

                return stay state
            with e ->
                //unexpected exception
                return! gotoNodeSystemFault ctx state e

        | AssumeAltMaster(RR ctx reply, clusterState) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            return! assumeAltMaster ctx state reply clusterState

        | AttachToCluster _ -> 
            //In this state this message is invalid
            ctx.LogWarning(sprintf "Unable to process %A in current node state." msg)

            return stay state

        | AttachToClusterSync(RR ctx reply, _) ->
            reply <| Exception(new InvalidOperationException(sprintf "Unable to process %A in current node state." msg))

            return stay state

        | DetachFromCluster ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED

            let! state' = clearState ctx state

            try
                let clusterId = state.ClusterInfo |> Option.fold (fun _ info -> info.ClusterId) "HEAD"
                ctx.LogInfo <| sprintf "Detaching from cluster %A..." clusterId

                ctx.LogInfo "Trigerring OnRemoveFromCluster..."

                do! state'.EventManager.OnRemoveFromCluster clusterId
            with e -> ctx.LogWarning e

            ctx.LogInfo "Node is detached from cluster."

            return goto nodeManagerInit state'

        | UpdateAltMasters newAltMasters ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                ctx.LogInfo <| sprintf "New alt masters: %A" newAltMasters

                //Throws
                //ArgumentException => state.ClusterInfo = None ;; SYSTEM FAULT
                return stay { state with ClusterInfo = Some { state.ClusterInfo.Value with AltMasters = newAltMasters |> Array.toList } }
            with e ->
                return! gotoNodeSystemFault ctx state e

        | MasterNodeLoss when state.IsAltMaster ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            ctx.LogInfo "MASTER NODE LOSS..."
            ctx.LogInfo "Current node is alt master."

            ctx.LogInfo "Stopping heart beats..."
            state.HeartBeat |> Option.iter (fun h -> h.Dispose())

            let altMasterIndex = state.AltMasterIndex
            if altMasterIndex = 0 then
                ctx.LogInfo "Current node will assume Master role. Recovering..."
                //If the current node is the first in the list of alt masters
                //then trigger master node recovery
                return! recoverMasterNode ctx state
            else
                ctx.LogInfo "Will wait for new master node..."

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

            ctx.LogInfo "MASTER NODE LOSS..."
            ctx.LogInfo "Stopping heart beats..."
            state.HeartBeat |> Option.iter (fun h -> h.Dispose())
            ctx.LogInfo "Waiting for new master node..."

            //Each node that is not a master node
            //waits for attachment to new master node (in nodeManagerInit state)
            //for twice the time it takes for master node recovery to occur
            //(master node dies and during recovery all alt masters die except the last one).
            //If the time runs out, then each node goes to failed state.
            return goto nodeManagerInit state //TODO!!! Make the timeout configurable (same as above)
                   |> onTimeout (30000 * state.ClusterStateLoggers.Length * 2) (gotoNodeSystemFault ctx state (SystemFailureException "Node was not re-attached to cluster aftern master node failure within the specified timespan."))

        | TriggerSystemFault -> return! gotoNodeSystemFault ctx state (SystemFailureException "Node received TriggerSystemFault")

        | InitCluster(RR ctx reply, clusterConfiguration) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            return! initCluster ctx state clusterConfiguration reply

        | GetNodeType(RR ctx reply) ->
            return! getNodeType ctx reply state

        //TODO!!! Extract to func (code duplication)
        | StopMonitoring clusterId ->
            try
                ctx.LogInfo <| sprintf "Stopping cluster %A monitoring..." clusterId

                match state.ManagedClusters.TryFind clusterId with
                | Some managedCluster ->
                    !managedCluster.ClusterHealthMonitor <-- DisableMonitoring
                | _ ->
                    ctx.LogWarning <| sprintf "Cluster %A is unknown" clusterId

                return stay state
            with e ->
                //unexpected error
                return! gotoNodeSystemFault ctx state e

        | StartMonitoring clusterId ->
            try
                ctx.LogInfo <| sprintf "Starting cluster %A monitoring..." clusterId

                match state.ManagedClusters.TryFind clusterId with
                | Some managedCluster ->
                    !managedCluster.ClusterHealthMonitor <-- EnableMonitoring
                | _ ->
                    ctx.LogWarning <| sprintf "Cluster %A is unknown" clusterId

                return stay state
            with e ->
                //unexpected error
                return! gotoNodeSystemFault ctx state e

        | FiniCluster clusterId ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //Throws
                //KeyNotFoundException => unknown cluster ;; trigger warning
                let! state' = finiCluster ctx state clusterId

                do! !state'.NodeEventExecutor <!- ExecSync

                return stay state'
            with e ->
                //unexpected error

                return! gotoNodeSystemFault ctx state e

        | FiniClusterSync(RR ctx reply, clusterId) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //Throws
                //KeyNotFoundException => unknown cluster ;; trigger warning
                let! state' = finiClusterSync ctx state clusterId

                do! !state'.NodeEventExecutor <!- ExecSync

                reply nothing

                return stay state'
            with e ->
                //unexpected error
                reply <| Exception e
                return! gotoNodeSystemFault ctx state e

        | DisposeFinilizedClusters ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                let state' = disposeClusters state

                return stay state'
            with e ->
                //unexpected error
                return! gotoNodeSystemFault ctx state e

        | SyncNodeEvents(RR ctx reply) ->
            return! syncNodeEvents ctx reply state
    }

//ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
and recoverMasterNode (ctx: BehaviorContext<_>) (state: NodeState) = 
    //Throws
    //InvalidOperationException => already a master node ;; SYSTEM FAULT
    let rec selectNewClusterStateLoggerLoop clusterState clusterStateWithoutMasterAndCurrent nodeFailuresDetected = async {
        try
            //select candidate
            //Throws
            //KeyNotFoundException => no nodes available ;; failover capacity reduced ;; TRIGGER MASSIVE WARNING
            let newAltMasterNode = ReliableActorRef.FromRef (ClusterManager.selectNewClusterStateLogger clusterStateWithoutMasterAndCurrent)

            ctx.LogInfo <| sprintf "New alt master candidate: %A" newAltMasterNode

            try
                ctx.LogInfo "Creating new alt master..."

                //FaultPoint
                //InvalidOperationException => already a master node ;; SYSTEM FAULT
                //FailureException => node failure ;; retry and remove node after recovery
                let! newClusterStateLogger = newAltMasterNode <!- fun ch -> AssumeAltMaster(ch, clusterState)

                ctx.LogInfo "Alt master created."

                //cluster state of clustermanager
                //the old altmaster is replaced by the new one
                //but the dead master node data must still be there
                //so that the clustermanager can perfrom the recovery properly
                let newClusterAltMasterNode = { NodeManager = newAltMasterNode; ClusterStateLogger = newClusterStateLogger }
                let newClusterState = clusterState.RemoveAltMasterNode state.NodeManager
                let newClusterState = newClusterState.AddAltMasterNode newClusterAltMasterNode

                ctx.LogInfo <| sprintf "New alt master set: %A" newClusterState.AltMasters

                try
                    ctx.LogInfo "Logging alt master updates..."
                    //update clusterstateloggers
                    //FaultPoint
                    //ClusterStateLogBroadcastException => some updates have failed (node failures) ;; remove failed nodes after recovery
                    //ClusterStateLogFailureException => total failure to update (all alt master node failures) ;; continue but TRIGGER MASSIVE WARNING
                    do! ClusterUpdate.logClusterUpdate newClusterState 
                        <| ClusterUpdate.FromUpdates [ ClusterRemoveAltMaster state.NodeManager 
                                                       ClusterAddAltMaster newClusterAltMasterNode
                                                       ClusterAddNode state.ClusterInfo.Value.MasterNode ]

                    return newClusterState, nodeFailuresDetected
                with ClusterStateLogBroadcastException(_, failedNodes) ->
                        return newClusterState, failedNodes@nodeFailuresDetected
                    | ClusterStateLogFailureException msg ->
                        ctx.LogWarning(sprintf "FAULT TOLERANCE COMPROMISED DURING MASTER NODE RECOVERY! COMPLETE FAILURE TO REPLICATE STATE. Any failure after this point not tolerated: %s" msg)

                        return newClusterState, (newClusterState.AltMasterNodes |> Seq.toList)@nodeFailuresDetected
            with (FailureException _) as e ->
                ctx.LogInfo <| sprintf "Failed to create alt master, will retry: %A" e

                return! selectNewClusterStateLoggerLoop 
                            clusterState 
                            (clusterStateWithoutMasterAndCurrent.RemoveNode newAltMasterNode) 
                            (newAltMasterNode.UnreliableRef::nodeFailuresDetected)
        with :? System.Collections.Generic.KeyNotFoundException ->
            ctx.LogWarning "UNABLE TO SELECT NEW ALTMASTER NODE. NO CANDIDATE NODES AVAILABLE. FAILOVER FACTOR REDUCED."

            return clusterState, nodeFailuresDetected
    }

    async {
        try
            ctx.LogInfo "Initializing Master Node recovery..."

            //NODE FAULT AT THIS POINT
            //Master node recovery on next alt master

            //throws if this is not alt master node ;; inconsistent state
            let lostMasterNode = state.ClusterInfo.Value.MasterNode

            //get cluster state
            let! clusterState = state.ClusterStateLogger.Value.Ref <!- GetClusterState

            ctx.LogInfo "Replicated master state obtained."

            //cluster state with master node removed 
            let clusterState' = clusterState.RemoveNode lostMasterNode
            //and without current node, but with current node activations
            let clusterStateWithoutMasterAndCurrent = clusterState'.RemoveFromNodes state.NodeManager

            //NODE FAULT AT THIS POINT
            //Alt master list updated
            //lostMasterNode is in node list of replicated cluster state, but
            // - is removed from node list in "trigger node recovery"
            //Master node recovery on next alt master:
            //lostMasterNode will be in node list, but
            // - is ignored in "notify cluster"
            // - is recovered and removed from node list in "trigger node recovery"

            ctx.LogInfo "Selecting new alt master..."

            //create new cluster state logger
            //Throws
            //InvalidOperationException => already a master node ;; SYSTEM FAULT
            let! clusterStateAltsUpdated, nodeFailuresDetected = selectNewClusterStateLoggerLoop clusterState clusterStateWithoutMasterAndCurrent []
            let clusterStateAltsUpdatedNewMaster = { clusterStateAltsUpdated with MasterNode = state.NodeManager }
            let clusterStateAltsUpdatedNewMaster = clusterStateAltsUpdatedNewMaster.RemoveFromNodes state.NodeManager
            let newClusterState = clusterStateAltsUpdatedNewMaster.AddNode lostMasterNode

            assert newClusterState.Db.ClusterNode.DataMap.ContainsKey(lostMasterNode)

            assert 
                (newClusterState.Db.ClusterNode.DataMap 
                    |> Map.toSeq |> Seq.map snd |> Seq.exists (fun cn -> cn.NodeManager = state.NodeManager) |> not)

            assert 
                (newClusterState.Db.ClusterAltMasterNode.DataMap 
                    |> Map.toSeq |> Seq.map snd |> Seq.exists (fun ca -> ca.NodeManager = state.NodeManager) |> not)

            ctx.LogInfo "Initializing ClusterManager..."

            //create cluster manager from logger data
            let state' = ClusterManager.createClusterManager state newClusterState (fun n -> Cluster.ClusterManager <-!- RemoveNode n)
            let clusterManager = state'.ManagedClusters.[newClusterState.ClusterId].ClusterManager.Ref
            
            ctx.LogInfo "ClusterManager initialized."

            //notify cluster
            let! nodeFailuresDetected' = async {
                try
                    ctx.LogInfo "Re-attaching cluster nodes..."

                    //FaultPoint
                    //BroadcastFailureException => total failure in broadcast ;; TRIGGER MASSIVE WARNING
                    //BroadcastPartialFailureException => some posts have failed ;; remove nodes after recovery
                    do! clusterStateWithoutMasterAndCurrent.Nodes
                        |> Seq.map ReliableActorRef.FromRef
                            //FaultPoint
                            //FailureException => node failure ;; remove node after recovery
                        |> Broadcast.post (AttachToCluster { Master = state.Address; AltMasters = newClusterState.AltMasters; ClusterId = newClusterState.ClusterId })
                        |> Broadcast.exec
                        |> Async.Ignore

                    //NODE FAULT AT THIS POINT
                    //Current node is master
                    //lostMasterNode not fully recovered: activation recoveries not performed
                    //lostMasterNode is in replicated node list
                    //Master node recovery by next alt master:
                    //lostMasterNode is in node list
                    // - will become a failed node in "notify cluster"
                    // - will be removed and recovered in "trigger recovery of detected failed nodes"
                    //current node will be next lostMasterNode
                    //current node will be in node list, but
                    // - will be ignored in "notify cluster"
                    // - will be recovered and removed from node list in "trigger node recovery"

                    return nodeFailuresDetected
                with BroadcastFailureException _ ->
                        //TRIGGER MASSIVE WARNING ;; do not remove nodes, let the cluster manager handle this one
                        ctx.LogWarning "FAILED TO ATTACH SLAVE NODES TO NEW MASTER NODE. ALL CLUSTER NODES FAILED."
                        return nodeFailuresDetected
                    | BroadcastPartialFailureException(_, failedNodes) ->
                        return (failedNodes |> List.map (fun (n, _) -> n :?> ActorRef<NodeManager>))@nodeFailuresDetected
            }

            Cluster.SetClusterManager(Some <| ReliableActorRef.FromRef clusterManager)

            ctx.LogInfo "Trigerring recovery of master node activations..."

            //trigger node recovery
            clusterManager <-- RemoveNode lostMasterNode

            ctx.LogInfo <| sprintf "Trigerring recovery of detected node failures: %A" nodeFailuresDetected'

            //trigger recovery of detected failed nodes
            for failedNode in nodeFailuresDetected' do clusterManager <-- RemoveNode failedNode

            //NODE FAULT AT THIS POINT
            //Current node is master
            //lostMasterNode may be recovered
            //current node is in replicated node list
            //Master node recovery by next alt master:
            //If lostMasterNode in replicated node list
            // - will become a failed node in "notify cluster"
            // - will be removed and recovered in "trigger recovery of detected failed nodes"
            //If lostMasterNode not in replicated node list
            // - lostMasterNode was recovered
            //current node will be next lostMasterNode
            //current node will be in node list, but
            // - will be ignored in "notify cluster"
            // - will be recovered and removed from node list in "trigger node recovery"

            //remove current node from replicated state node list
            do! ClusterUpdate.logClusterUpdate newClusterState 
                    (ClusterUpdate.FromUpdates [ ClusterRemoveFromNodes state.NodeManager ])
                |> Async.Catch
                |> Async.Ignore

            //NODE FAULT AT THIS POINT
            //Current node is master
            //lostMasterNode may be recovered
            //current node not in replicated node list
            //Master node recovery by next alt master:
            //If lostMasterNode in replicated node list
            // - will become a failed node in "notify cluster"
            // - will be removed and recovered in "trigger recovery of detected failed nodes"
            //If lostMasterNode not in replicated node list
            // - lostMasterNode was recovered
            //current node will be next lostMasterNode

            ctx.LogInfo <| sprintf "New cluster id: %A" newClusterState.ClusterId

            state'.NodeEventExecutor.Stop()
            let state'' = { state' with NodeEventExecutor = NodeState.CreateNodeEventExecutorActor() }
            do! state''.NodeEventExecutor.Ref <!- ExecSync

            state''.NodeEventExecutor.Ref <--
                ExecEvent(
                    ctx,
                    state''.EventManager.OnMaster(newClusterState.ClusterId)
                )

            ctx.LogInfo "MASTER NODE FAILURE RECOVERY COMPLETE"

            state''.NodeEventExecutor.Ref <--
                ExecEvent(
                    ctx,
                    state''.EventManager.OnMasterRecovered(newClusterState.ClusterId)
                )

            return goto nodeManagerProper { state'' with ClusterInfo = None }
        with e ->
            //unexpected exception at this point
            return! gotoNodeSystemFault ctx state e
    }

and nodeManagerSystemFault (ctx: BehaviorContext<NodeManager>) (state: NodeState) (msg: NodeManager) =
    let reply r = r <| Exception(SystemFailureException "System is in failed state.")
    let warning () = ctx.LogWarning "System is in failed state. No message is processed."
    async {
        match msg with
        | Activate(RR ctx r, _, _, _, _) -> reply r
        | DeActivate(RR ctx r, _, _) -> reply r
        | Resolve(RR ctx r, _) -> reply r
        | TryResolve(RR ctx r, _) -> reply r
        | TryGetManagedCluster(RR ctx r, _) -> reply r
        | AssumeAltMaster(RR ctx r, _) -> reply r
        | InitCluster(RR ctx r, _) -> reply r
        | GetNodeType(RR ctx r) -> reply r
        | AttachToClusterSync(RR ctx r, _) -> reply r
        | SyncNodeEvents(RR ctx r) -> reply r
        | FiniClusterSync(RR ctx r, _) -> reply r
        | FiniCluster _ //TODO!!! Actually finilize the cluster
        | StartMonitoring _
        | StopMonitoring _
        | DisposeFinilizedClusters _
        | AttachToCluster _
        | DetachFromCluster
        | UpdateAltMasters _
        | MasterNodeLoss
        | NotifyDependencyLost _
        | NotifyDependencyRecovered _
        | TriggerSystemFault -> warning()

        return stay state
    }

let createNodeManager (nodeConfig: NodeConfiguration) = 
    async {
        try
            Log.logInfo "NODE MANAGER INIT..."
            let state = NodeState.New nodeConfig

            let nodeManager =
                Actor.bind <| FSM.fsmBehavior (goto nodeManagerInit state)
                |> Actor.subscribeLog (Default.actorEventHandler Default.fatalActorFailure String.Empty)
                |> Actor.rename "nodeManager"
                |> Actor.publish [UTcp()] //TODO!!! Add bidirectional for diagnostics from shell
                |> Actor.start

            Log.logInfo "NodeManager started."

            NodeRegistry.Init nodeManager.Ref

            Log.logInfo "NodeRegistry initialized."

            Cluster.SetNodeManager(Some nodeManager.Ref)
            Cluster.SetDefinitionRegistry(Some nodeConfig.DefinitionRegistry)

            Log.logInfo "Running node init hooks..."

            do! nodeConfig.EventManager.OnInit()

            Log.logInfo "Node init hooks completed."

            Log.logInfo "NODE MANAGER INITIALIZED."
            Log.logInfo "{m}node READY..."

            return nodeManager
        with e -> 
            Log.logException e "NODE MANAGER INIT FAULT"
            return! Async.Raise e
    }

let initNode (nodeConfig: NodeConfiguration) = 
    async {
        do! Async.Ignore (createNodeManager nodeConfig)

        while true do do! Async.Sleep 5000
    }
