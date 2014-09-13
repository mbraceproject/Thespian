module Nessos.Thespian.Cluster.ClusterUpdate

open Nessos.Thespian
open Nessos.Thespian.Utilities

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

