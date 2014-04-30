//Prelude
//Evaluate in fsi before all else
#r "bin/debug/Nessos.Thespian.dll"
#r "bin/debug/Nessos.Thespian.Remote.dll"
#r "../Nessos.MBrace.ImemDb/bin/debug/Nessos.MBrace.ImemDb.dll"
#r "../Nessos.MBrace.Utils/bin/debug/Nessos.MBrace.Utils.dll"

open System
open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.MBrace.ImemDb
open Nessos.MBrace.Utils
open Nessos.MBrace.Utils.Reversible

module AsyncFSM =

    let rec fsm (inputGenerator: Async<'I>) (transitionF: 'S -> 'I -> Async<'S>) (isFiniState: 'S -> bool) (state: 'S) : Async<'S> =
        async {
            let! input = inputGenerator

            let! state' = transitionF state input
            if isFiniState state' then return state'
            else return! fsm inputGenerator transitionF isFiniState state'
        }


type FaultType =
    | NodeUnknown //cannot ever see the node; never get response from address
    | NodeLost //could see the node but lost it
    | NodeUnresponsive //delivery failure
    | ActorUnknown //actor is always unknown to the receiving node; indicates a stopped actor
    | ActorLost //actor became unknown to the receiving node; indicates a failed actor
    | ActorUnresponsive //not getting replies
    | ActorUnreliable //became known but there was a timeout

type Fault = //TODO!!! Include exception
    | Transient of FaultType
    | Permanent of FaultType

type Attempt<'T> = Success of 'T | Failure of Fault

type RelieablePostExecutor<'T>(retriesPerError : int, retryInterval : int, attemptTimeout : int) =
    let faultCardinality = 7 //how many types of fault are there
    let maxRetries = retriesPerError * faultCardinality
    
    let initState = [], retriesPerError
    let isFiniState = function (Success _)::_, _ | (Failure _)::_, 0 -> true | _ -> false 

    let transitionF state (input : Choice<'T, exn>) = 
        let faults, n = state
        match faults, n, input with
        //final states
        | faults, _, Choice1Of2 r -> (Success r)::faults, 0
        | Failure(Permanent _)::_, _, _ -> faults, 0
        | _, 0, _ -> faults, 0
        //init state
        | [], n, Choice2Of2(:? TimeoutException) -> [Failure(Transient ActorUnresponsive)], n - 1
        | [], n, Choice2Of2(UnknownRecipientException _) -> [Failure(Transient ActorUnknown)], n - 1
        | [], n, Choice2Of2(CommunicationException _) -> [Failure(Transient NodeUnknown)], n - 1
        //identity states
        | Failure(Transient(ActorUnresponsive | ActorUnreliable))::_, n, Choice2Of2(:? TimeoutException)
        | Failure(Transient(ActorLost | ActorUnknown))::_, n, Choice2Of2(UnknownRecipientException _) 
        | Failure(Transient NodeUnresponsive)::_, n, Choice2Of2(DeliveryException _) 
        | Failure(Transient(NodeUnknown | NodeLost))::_, n, Choice2Of2(CommunicationException _) -> faults, n - 1
        //transition states
        | Failure(Transient ActorLost)::_, _, Choice2Of2(:? TimeoutException) -> Failure(Transient ActorUnreliable)::faults, retriesPerError
        | Failure(Transient ActorLost)::_, _, Choice2Of2(DeliveryException _) -> Failure(Transient NodeUnresponsive)::faults, retriesPerError
        | Failure(Transient ActorLost)::_, _, Choice2Of2(CommunicationException _) -> Failure(Transient NodeLost)::faults, retriesPerError
        | Failure(Transient (ActorUnresponsive | ActorUnreliable))::_, _, Choice2Of2(UnknownRecipientException _) -> Failure(Transient ActorLost)::faults, retriesPerError
        | Failure(Transient (ActorUnresponsive | ActorUnreliable))::_, _, Choice2Of2(DeliveryException _) -> Failure(Transient NodeUnresponsive)::faults, retriesPerError
        | Failure(Transient (ActorUnresponsive | ActorUnreliable))::_, _, Choice2Of2(CommunicationException _) -> Failure(Transient NodeLost)::faults, retriesPerError
        | Failure(Transient NodeLost)::_, _, Choice2Of2(UnknownRecipientException _) -> Failure(Transient ActorLost)::faults, retriesPerError
        | Failure(Transient NodeLost)::_, _, Choice2Of2(DeliveryException _) -> Failure(Transient NodeUnresponsive)::faults, retriesPerError
        | Failure(Transient NodeLost)::_, _, Choice2Of2(:? TimeoutException) -> Failure(Transient ActorUnreliable)::faults, retriesPerError
        | Failure(Transient NodeUnknown)::_, _, Choice2Of2(UnknownRecipientException _) -> Failure(Transient ActorUnknown)::faults, retriesPerError
        | Failure(Transient NodeUnknown)::_, _, Choice2Of2(DeliveryException _) -> Failure(Transient NodeUnresponsive)::faults, retriesPerError
        | Failure(Transient NodeUnknown)::_, _, Choice2Of2(:? TimeoutException) -> Failure(Transient ActorUnresponsive)::faults, retriesPerError
        | Failure(Transient NodeUnresponsive)::_, _, Choice2Of2(UnknownRecipientException _) -> Failure(Transient ActorUnknown)::faults, retriesPerError
        | Failure(Transient NodeUnresponsive)::_, _, Choice2Of2(CommunicationException _) -> Failure(Transient NodeLost)::faults, retriesPerError
        | Failure(Transient NodeUnresponsive)::_, _, Choice2Of2(:? TimeoutException) -> Failure(Transient ActorUnresponsive)::faults, retriesPerError
        | _ -> failwith "Retry strategy fault."

    member __.ExecPost(post : Async<'T>) : Async<Choice<'T, Fault list>> =
        let inputGenerator = async {
            //we want to post but with a particular timeout
            let! post'= Async.StartChild(post, attemptTimeout)

            return! Async.Catch post'
        }
        let initState' = initState, maxRetries
        let isFiniState' (state, n) = isFiniState state || n = 0
        let transitionF' (state, n) input = async {
            let state' = transitionF state input
            match state' with
            | (Failure _)::_, _ ->
                do! Async.Sleep retryInterval
            | _ -> ()

            return state', n - 1
        }

        let liftToFaults = List.choose (function Failure f -> Some f | _ -> None)

        async {
            let! (attempts, _), _ = AsyncFSM.fsm inputGenerator transitionF' isFiniState' initState'

            return match attempts with
                   | (Success r)::_ -> Choice1Of2 r
                   | _ -> Choice2Of2(liftToFaults attempts)
        }

exception FailureException of Fault list * ActorRef

//TODO!!! Do all actorRef post methods
type ReliableActorRef<'T>(baseRef : ActorRef<'T>, ?retriesPerError : int, ?retryInterval : int, ?attemptTimeout : int) =
    inherit ActorRef<'T>(baseRef)

    let retriesPerError = defaultArg retriesPerError 3
    let retryInterval = defaultArg retryInterval 1500
    let attemptTimeout = defaultArg attemptTimeout 30000

    override __.Post(msg : 'T) : unit = 
        let reliablePostExecutor = new RelieablePostExecutor<_>(retriesPerError, retryInterval, attemptTimeout)

        let result = reliablePostExecutor.ExecPost(async { return baseRef.Post msg }) |> Async.RunSynchronously
        match result with
        | Choice1Of2 r -> r
        | Choice2Of2 faults -> raise <| FailureException(faults, baseRef)

    override __.PostWithReply(msgBuilder : IReplyChannel<'R> -> 'T) : Async<'R> = 
        async {
            let reliablePostExecutor = new RelieablePostExecutor<_>(retriesPerError, retryInterval, attemptTimeout)

            let! result = reliablePostExecutor.ExecPost(baseRef.PostWithReply msgBuilder)
            return match result with
                   | Choice1Of2 r -> r
                   | Choice2Of2 faults -> raise <| FailureException(faults, baseRef)
        }


type ReliableActorRef = //TODO!!! Handle the case: ReliableActorRef.FromRef(reliableActorRef)
    static member FromRef(actorRef : ActorRef<'T>, ?retriesPerError : int, ?retryInterval : int, ?attemptTimeout : int) : ReliableActorRef<'T> =
        new ReliableActorRef<_>(actorRef, ?retriesPerError = retriesPerError, ?retryInterval = retryInterval, ?attemptTimeout = attemptTimeout)





