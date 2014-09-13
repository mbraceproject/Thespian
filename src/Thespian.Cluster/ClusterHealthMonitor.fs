module Nessos.Thespian.Cluster.ClusterHealthMonitor

open System
open System.Diagnostics

open Nessos.Thespian
open Nessos.Thespian.Utilities
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol

let private addressToNodeHeartBeat (address: Address) =
    let serializer = Serialization.defaultSerializer.Name
    ActorRef.fromUri (sprintf "utcp://%A/nodeHeartBeat" address)

let private addressToNodeManager (address: Address) =
    let serializer = Serialization.defaultSerializer.Name
    ActorRef.fromUri (sprintf "utcp://%A/nodeManager" address)

let private falsePositiveTest address = async {
    let! r = ReliableActorRef.FromRef (addressToNodeHeartBeat address) <!- BeatBack |> Async.Catch
    return match r with
           | Choice1Of2 _ -> None
           | Choice2Of2 _ -> Some address
}

let private healthCheck (notifyDeadNode: ActorRef<NodeManager> -> Async<unit>) (ctx: BehaviorContext<_>) (state: ClusterHealthMonitorState) = 
    async {
        //ctx.LogInfo "Health check..."

        let now = DateTime.Now

        let deadCandidates =
            state.Nodes
            |> Map.toSeq
            |> Seq.filter (fun (_, lastHeartBeat) ->
                let timeDiff = now - lastHeartBeat

                timeDiff.TotalMilliseconds > (state.HeartBeatInterval.TotalMilliseconds * state.DetectionFactor)
            )
            |> Seq.toList

        let! deads = deadCandidates |> List.map fst |> List.chooseAsync falsePositiveTest

        let falsePositives = deadCandidates.Length - deads.Length
        if falsePositives > 0 then ctx.LogInfo <| sprintf "%d false positive node deaths detected." falsePositives

        if deads.Length <> 0 then
            ctx.LogInfo <| sprintf "Declaring dead nodes: %A" deads

        for deadNode in deads |> Seq.map addressToNodeManager do
            do! notifyDeadNode deadNode

        return {
            state with Nodes = deads |> Seq.fold (fun nodes deadNode -> Map.remove deadNode nodes) state.Nodes
        }
    }

//Any fault => trigger SYSTEM FAULT
let private updateState (ctx: BehaviorContext<_>) (state: ClusterHealthMonitorState) (msg: ClusterHealthMonitor) =
    async {
        match msg with
        | HeartBeat nodeAddress ->
            return { state with Nodes = Map.add nodeAddress DateTime.Now state.Nodes }

        | DisableMonitoring ->
            do! state.Nodes
                |> Map.toSeq
                |> Seq.map fst
                |> Seq.map addressToNodeHeartBeat
                |> Broadcast.post StopBeating
                |> Broadcast.ignoreFaults Broadcast.allFaults
                |> Broadcast.exec
                |> Async.Ignore

            return { state with Enabled = false }

        | EnableMonitoring ->
            do! state.Nodes
                |> Map.toSeq
                |> Seq.map fst
                |> Seq.map addressToNodeHeartBeat
                |> Broadcast.post (StartBeating state.HeartBeatInterval)
                |> Broadcast.ignoreFaults Broadcast.allFaults
                |> Broadcast.exec
                |> Async.Ignore

            return { state with Enabled = true }

        | StartMonitoringNode nodeAddress ->
            try
                ctx.LogInfo (sprintf "Starting to monitor node %A..." nodeAddress)

                //FaultPoint
                //FailureException => ignore and do not add
                ReliableActorRef.FromRef (addressToNodeHeartBeat nodeAddress) <-- StartBeating state.HeartBeatInterval

                return { state with Nodes = Map.add nodeAddress DateTime.Now state.Nodes }
            with FailureException _ -> 
                ctx.LogWarning <| sprintf "Node %A is inaccessile."
                return state

        | StopMonitoringNode nodeAddress ->
            try
                ctx.LogInfo (sprintf "Stopping to monitor node %A..." nodeAddress)

                //FaultPoint
                //FailureException => ignore and do not add
                ReliableActorRef.FromRef (addressToNodeHeartBeat nodeAddress) <-- StopBeating
            with FailureException _ -> ctx.LogWarning <| sprintf "Node %A is inaccessile."
            
            return { state with Nodes = Map.remove nodeAddress state.Nodes }
    }

let clusterHealthMonitorBehavior (notifyDeadNode: ActorRef<NodeManager> -> Async<unit>) 
                                 (initState: ClusterHealthMonitorState) 
                                 (self: Actor<ClusterHealthMonitor>) =
    let ctx = new BehaviorContext<_>(self)
    let stopwatch = new Stopwatch()

    let rec messageLoop state =
        async {
            if self.PendingMessages <> 0 then
                let! msg = self.Receive()
                let! state' = updateState ctx state msg
                return! messageLoop state'
            else return state
        }

    let rec behaviorLoop state =
        async {
            try
                stopwatch.Start()

                //process heartbeats and other msgs
                let! state' = messageLoop state

                //check health of cluster
                let! state'' = healthCheck notifyDeadNode ctx state'

                stopwatch.Stop()

                let iterationTime = stopwatch.Elapsed

                let! state''' = async {
                    if iterationTime < state''.MinimumHeartBeatInterval then
                        do! Async.Sleep <| int (state.HeartBeatInterval - iterationTime).TotalMilliseconds
                        return state''
                    elif iterationTime >= state''.HeartBeatInterval then
                        //heart rate too big
                        //need to increase the heart beat interval

                        let state''' = { state'' with HeartBeatInterval = TimeSpan.FromMilliseconds(iterationTime.TotalMilliseconds * state''.AdaptationFactor) }

                        ctx.LogInfo <| sprintf "Increasing heart beat interval from %O to %O..." state''.HeartBeatInterval state'''.HeartBeatInterval

                        do! state'''.Nodes
                            |> Map.toSeq
                            |> Seq.map fst
                            |> Seq.map addressToNodeHeartBeat
                            |> Broadcast.post (SetHeartBeatInterval state'''.HeartBeatInterval)
                            |> Broadcast.ignoreFaults Broadcast.allFaults
                            |> Broadcast.exec
                            |> Async.Ignore

                        return state'''
                    elif iterationTime.TotalMilliseconds < (state''.HeartBeatInterval.TotalMilliseconds / state''.AdaptationFactor) then
                        //hear rate to small
                        //need to decrease the heart beat interval

                        let newInterval = min (iterationTime.TotalMilliseconds / state''.AdaptationFactor) state''.MinimumHeartBeatInterval.TotalMilliseconds

                        let state''' = { state'' with HeartBeatInterval = TimeSpan.FromMilliseconds newInterval }

                        ctx.LogInfo <| sprintf "Decreasing heart beat interval from %O to %O..." state''.HeartBeatInterval state'''.HeartBeatInterval

                        do! state'''.Nodes
                            |> Map.toSeq
                            |> Seq.map fst
                            |> Seq.map addressToNodeHeartBeat
                            |> Broadcast.post (SetHeartBeatInterval state'''.HeartBeatInterval)
                            |> Broadcast.ignoreFaults Broadcast.allFaults
                            |> Broadcast.exec
                            |> Async.Ignore

                        do! Async.Sleep <| int (state.HeartBeatInterval - iterationTime).TotalMilliseconds

                        return state'''
                    else
                        do! Async.Sleep <| int (state.HeartBeatInterval - iterationTime).TotalMilliseconds
                        return state''
                }

                stopwatch.Reset()

                return! behaviorLoop state'''
            with e ->
                ctx.LogInfo "Unexpected error in cluster health monitoring service."
                ctx.LogError e
                ctx.LogInfo "TRIGGERING SYSTEM FAULT..."
                Cluster.ClusterManager <-- FailCluster e
        }

    behaviorLoop initState

