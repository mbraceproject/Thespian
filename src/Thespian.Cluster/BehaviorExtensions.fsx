//Evaluate in fsi before all else
#r "bin/debug/Nessos.Thespian.dll"
#r "bin/debug/Nessos.Thespian.Remote.dll"
#r "../Nessos.MBrace.Utils/bin/debug/Nessos.MBrace.Utils.dll"

open System
open System.Threading
open Nessos.Thespian
open Nessos.MBrace.Utils

let (|MessageHandlingException2|_|) (e: exn) = 
    match e with
    | :? MessageHandlingException -> Some e.InnerException
    | _ -> None

module Async =
    let withTimeout (timeout: int) (computation: Async<'T>): Async<'T> =
        async {
            let! childComputation = Async.StartChild(computation, timeout)

            return! childComputation
        }

    let tryWithTimeout (timeout: int) (computation: Async<'T>): Async<'T option> =
        async {
            let! r = computation |> withTimeout timeout |> Async.Catch
            match r with
            | Choice1Of2 v -> return Some v
            | Choice2Of2(:? TimeoutException) -> return None
            | Choice2Of2 e -> return! Async.Raise e
        }

type BehaviorContext<'T>(self: Actor<'T>) =
    
    member __.Self = self.Ref

    member __.LogInfo i = self.LogInfo i
    member __.LogWarning w = self.LogWarning w
    member __.LogError e = self.LogError e
    member __.LogEvent(l, e) = self.LogEvent(l, e)

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

        and fsmTimeoutLoop (s: 'S) (t: TransitionBehavior<'S, 'T>) =
            async {
                let! m = self.Receive()

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
