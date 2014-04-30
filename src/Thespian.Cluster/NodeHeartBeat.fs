module Nessos.Thespian.Cluster.NodeHeartBeat

open System
open Nessos.Thespian
open Nessos.Thespian.Remote.TcpProtocol

let nodeHeartBeatBehavior (healthMonitor: ReliableActorRef<ClusterHealthMonitor>) (self: Actor<NodeHeartBeat>) =
    let ctx = new BehaviorContext<_>(self)

    let address = ActorRef.toUniTcpAddress self.Ref |> Option.get

    let rec messageLoop (heartBeatInterval: TimeSpan option) =
        async {
            if self.CurrentQueueLength <> 0 then
                let! msg = self.Receive()

                match msg with
                | SetHeartBeatInterval heartBeatInterval'
                | StartBeating heartBeatInterval' ->
                    return! messageLoop (Some heartBeatInterval')
                | StopBeating ->
                    return! messageLoop None
                | BeatBack(RR ctx reply) ->
                    reply nothing
                    return heartBeatInterval
            else return heartBeatInterval
        }

    let rec behaviorLoop (heartBeatInterval: TimeSpan option) = 
        async {
            let! heartBeatInterval' = messageLoop heartBeatInterval

            match heartBeatInterval' with
            | Some interval ->
                do! Async.Sleep (int interval.TotalMilliseconds)

                try
                    //FaultPoint
                    //FailureException => master node failure ;; trigger master node loss
                    //e => unexpected failure ;; trigger node system fault
                    healthMonitor <-- HeartBeat address

                    return! behaviorLoop heartBeatInterval'
                with FailureException _ ->
                        Cluster.NodeManager <-- MasterNodeLoss
                    | e ->
                        ctx.LogInfo "Unexpected error occured."
                        ctx.LogError e
                        Cluster.NodeManager <-- TriggerSystemFault
            | None ->
                let! msg = self.Receive()
                match msg with
                | SetHeartBeatInterval heartBeatInterval'
                | StartBeating heartBeatInterval' ->
                    return! behaviorLoop (Some heartBeatInterval')
                | StopBeating ->
                    return! behaviorLoop None
                | BeatBack(RR ctx reply) -> 
                    reply nothing
                    return! behaviorLoop None
        }

    behaviorLoop None
