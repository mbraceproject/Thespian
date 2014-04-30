module Nessos.Thespian.Cluster.ClusterStateLogger

open Nessos.Thespian
open Nessos.Thespian.Remote.TcpProtocol.Unidirectional
open Nessos.Thespian.Cluster.BehaviorExtensions

let private clusterStateLoggerBehavior (ctx: BehaviorContext<ClusterStateLogger>) (state: ClusterState) (msg: ClusterStateLogger) =
    async {
        match msg with
        | UpdateClusterState clusterUpdate ->
            return ClusterUpdate.Update(state, clusterUpdate)

        | GetClusterState(RR ctx reply) ->
            reply <| Value state
            return state
    }

let createClusterStateLogger clusterState =
    Actor.bind <| Behavior.stateful clusterState clusterStateLoggerBehavior
    |> Actor.rename ("clusterStateLogger." + clusterState.ClusterId)
    |> Actor.publish [UTcp()]
    |> Actor.start
