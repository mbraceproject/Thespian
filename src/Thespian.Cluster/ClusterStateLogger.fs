module Nessos.Thespian.Cluster.ClusterStateLogger

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Cluster.BehaviorExtensions

let private clusterStateLoggerBehavior (ctx: BehaviorContext<ClusterStateLogger>) (state: ClusterState) (msg: ClusterStateLogger) =
    async {
        match msg with
        | UpdateClusterState clusterUpdate ->
            return ClusterUpdate.Update(state, clusterUpdate)

        | GetClusterState(RR ctx reply) ->
            reply <| Ok state
            return state
    }

let createClusterStateLogger clusterState =
    Actor.bind <| Behavior.stateful clusterState clusterStateLoggerBehavior
    |> Actor.rename ("clusterStateLogger." + clusterState.ClusterId)
    |> Actor.publish [Protocols.utcp()]
    |> Actor.start
