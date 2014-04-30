[<AutoOpen>]
module Nessos.Thespian.Cluster.BehaviorExtensions

open System
open System.Threading
open Nessos.Thespian
open Nessos.Thespian.AsyncExtensions

let (|MessageHandlingException2|_|) (e: exn) = 
    match e with
    | :? MessageHandlingException -> Some e.InnerException
    | _ -> None

module Async =
    let tryWithTimeout (timeout: int) (computation: Async<'T>): Async<'T option> =
        async {
            //let! childComputation = Async.StartChild(computation, timeout)
            let resultComputation = async { let! r' = computation in return Some (Some r') }
            let timeoutComputation = async {
                do! Async.SleepSafe timeout
                return Some None
            }

            let! r = Async.Choice [ resultComputation; timeoutComputation ]
            return match r with
                   | Some result -> result
                   | None -> failwith "Impossible!"        }

//    let tryWithTimeout (timeout: int) (computation: Async<'T>): Async<'T option> =
//        async {
//            let! r = computation |> withTimeout timeout |> Async.Catch
//            match r with
//            | Choice1Of2 v -> return Some v
//            | Choice2Of2(:? TimeoutException) -> return None
//            | Choice2Of2 e -> return! Async.Raise e
//        }

type BehaviorContext<'T>(self: ActorBase, selfRef: ActorRef<'T>) =

    new (self: Actor<'T>) = new BehaviorContext<'T>(self, self.Ref)

    member private __.Actor = self

    member __.Self = selfRef

    member __.LogInfo i = self.LogInfo i
    member __.LogWarning w = self.LogWarning w
    member __.LogError e = self.LogError e
    member __.LogEvent(l, e) = self.LogEvent(l, e)

    static member map (mapF: 'T -> 'U) (ctx: BehaviorContext<'U>) =
        new BehaviorContext<'T>(ctx.Actor, ctx.Self |> PowerPack.ActorRef.map mapF)
            

module Behavior =
    let stateful (state: 'U) (behavior: BehaviorContext<'T> -> 'U -> 'T -> Async<'U>) (self: Actor<'T>) =
        let ctx = new BehaviorContext<'T>(self)
        let rec stateful' state = async {
            let! msg = self.Receive()

            let! state' = behavior ctx state msg

            return! stateful' state'
        }

        stateful' state

    let stateless (behavior: BehaviorContext<'T> -> 'T -> Async<unit>) =
        stateful () (fun ctx _ msg -> behavior ctx msg)

    let union (state1: 'S1) (behavior1: BehaviorContext<'T1> -> 'S1 -> 'T1 -> Async<'S1>)
              (state2: 'S2) (behavior2: BehaviorContext<'T2> -> 'S2 -> 'T2 -> Async<'S2>)
              (self: Actor<Choice<'T1, 'T2>>) =
        let ctx1 = new BehaviorContext<'T1>(self, self.Ref |> PowerPack.ActorRef.map Choice1Of2)
        let ctx2 = new BehaviorContext<'T2>(self, self.Ref |> PowerPack.ActorRef.map Choice2Of2)
        let rec stateful (state1, state2) = async {
            let! msg = self.Receive()
            match msg with
            | Choice1Of2 msg1 ->
                let! state1' = behavior1 ctx1 state1 msg1
                return! stateful (state1', state2)
            | Choice2Of2 msg2 ->
                let! state2' = behavior2 ctx2 state2 msg2
                return! stateful (state1, state2')
        }
        stateful (state1, state2)

module FSM =

    type TransitionBehavior<'S, 'T> = BehaviorContext<'T> -> 'S -> 'T -> Async<Transition<'S, 'T>>

    and Transition<'S, 'T> = 
        | Id of 'S 
        | Next of 'S * TransitionBehavior<'S, 'T>
        //WithTimeout(timeout, inTimeTransition, outOfTimeTransition)
        | WithTimeout of int * Transition<'S, 'T> * Async<Transition<'S, 'T>>

    let fsmBehavior n (self: Actor<'T>) =
        //TODO!!! What should happen with exceptions here??

        let ctx = new BehaviorContext<'T>(self)

        let rec fsmNormal (s: 'S) (t: TransitionBehavior<'S, 'T>) =
            async {
                let! m = self.Receive()

                let! transition = t ctx s m

                return! transit t transition
            }

        and transit b t =
            async {
                match t with
                | Id s' -> return! fsmNormal s' b
                | Next(s', b') -> return! fsmNormal s' b'
                | WithTimeout(timeout, Id s', outOfTime) ->
                    return! fsmTimeout timeout b (fsmTimeoutLoop s' b) outOfTime
                | WithTimeout(timeout, Next(s', b'), outOfTime) ->
                    return! fsmTimeout timeout b' (fsmTimeoutLoop s' b') outOfTime
                | WithTimeout(timeout, t', outOfTime) ->
                    return! fsmTimeout timeout b (transit b t') outOfTime
            }

        and fsmTimeout timeout b op outOfTime =
            async {
                let! r = Async.tryWithTimeout timeout op
                match r with
                | None ->
                    let! t = outOfTime
                    return! transit b t
                | Some t ->
                    return! transit b t
            }

        and pollReceive () = 
            async {
                if self.CurrentQueueLength > 0 then return! self.Receive()
                else
                    do! Async.SleepSafe(500)
                    return! pollReceive()
            }

        and fsmTimeoutLoop (s: 'S) (t: TransitionBehavior<'S, 'T>) =
            async {
                //let! m = self.Receive()
                let! m = pollReceive ()

                let! transition = t ctx s m

                match transition with
                | Id s' -> return! fsmTimeoutLoop s t
                | _ -> return transition
            }

        match n with
        | Next(s, b) -> fsmNormal s b
        | WithTimeout(timeout, Next(s, b), outOfTime) -> fsmTimeout timeout b (fsmTimeoutLoop s b) outOfTime
        | _ -> failwith "Invalid initial Fsm state"
        |> Async.Ignore
        

    let goto f s = Next(s, f)
    let stay s = Id s
    let onTimeout timeout outOfTime onTime = WithTimeout(timeout, onTime, outOfTime)


