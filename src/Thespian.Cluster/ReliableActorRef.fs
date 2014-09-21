[<AutoOpen>]
module Nessos.Thespian.Cluster.ReliableActorRef

open System
open System.Runtime.Serialization
open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Utils.ImemDb
open Nessos.Thespian.Cluster

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
    | Transient of FaultType * exn
    | Permanent of FaultType * exn

exception RetryFault of exn * Fault list

type Attempt<'T> = Success of 'T | Failure of Fault

type RelieablePostExecutor<'T>(retriesPerError : int, retryInterval : int, attemptTimeout : int) =
    let faultCardinality = 7 //how many types of fault are there
    let maxRetries = retriesPerError * faultCardinality
    
    let initState = [], retriesPerError
    let isFiniState = function (Success _)::_, _ | (Failure _)::_, 0 -> true | _ -> false 

    let liftToFaults = List.choose (function Failure f -> Some f | _ -> None)

    let transitionF state (input : Choice<'T, exn>) = 
        let faults, n = state
        match faults, n, input with
        | _, _, Choice2Of2(MessageHandlingException _ as e) -> raise e
        //final states
        | faults, _, Choice1Of2 r -> (Success r)::faults, 0
        | Failure(Permanent _)::_, _, _ -> faults, 0
        | _, 0, _ -> faults, 0
        //init state
        | [], n, Choice2Of2(:? TimeoutException as e) -> [Failure(Transient(ActorUnresponsive, e))], n - 1
        | [], n, Choice2Of2(UnknownRecipientException _ as e) -> [Failure(Transient(ActorUnknown, e))], n - 1
        | [], n, Choice2Of2(CommunicationException _ as e) -> [Failure(Transient(NodeUnknown, e))], n - 1
        //identity states
        | Failure(Transient((ActorUnresponsive | ActorUnreliable as ft), _))::_, n, Choice2Of2(:? TimeoutException as e) ->
            (Failure(Transient(ft, e)))::faults, n - 1
        | Failure(Transient((ActorLost | ActorUnknown as ft), _))::_, n, Choice2Of2(UnknownRecipientException _ as e)
        | Failure(Transient((NodeUnresponsive as ft), _))::_, n, Choice2Of2(DeliveryException _ as e) 
        | Failure(Transient((NodeUnknown | NodeLost as ft), _))::_, n, Choice2Of2(CommunicationException _ as e) -> 
            (Failure(Transient(ft, e)))::faults, n - 1
        //transition states
        | Failure(Transient(ActorLost, _))::_, _, Choice2Of2(:? TimeoutException as e) -> 
            Failure(Transient(ActorUnreliable, e))::faults, retriesPerError
        | Failure(Transient(ActorLost, _))::_, _, Choice2Of2(DeliveryException _ as e) -> 
            Failure(Transient(NodeUnresponsive, e))::faults, retriesPerError
        | Failure(Transient(ActorLost, _))::_, _, Choice2Of2(CommunicationException _ as e) -> 
            Failure(Transient(NodeLost, e))::faults, retriesPerError
        | Failure(Transient((ActorUnresponsive | ActorUnreliable), _))::_, _, Choice2Of2(UnknownRecipientException _ as e) -> 
            Failure(Transient(ActorLost, e))::faults, retriesPerError
        | Failure(Transient((ActorUnresponsive | ActorUnreliable), _))::_, _, Choice2Of2(DeliveryException _ as e) -> 
        Failure(Transient(NodeUnresponsive, e))::faults, retriesPerError
        | Failure(Transient((ActorUnresponsive | ActorUnreliable), _))::_, _, Choice2Of2(CommunicationException _ as e) -> 
            Failure(Transient(NodeLost, e))::faults, retriesPerError
        | Failure(Transient(NodeLost, _))::_, _, Choice2Of2(UnknownRecipientException _ as e) -> 
            Failure(Transient(ActorLost, e))::faults, retriesPerError
        | Failure(Transient(NodeLost, _))::_, _, Choice2Of2(DeliveryException _ as e) -> 
            Failure(Transient(NodeUnresponsive, e))::faults, retriesPerError
        | Failure(Transient(NodeLost, _))::_, _, Choice2Of2(:? TimeoutException as e) -> 
            Failure(Transient(ActorUnreliable, e))::faults, retriesPerError
        | Failure(Transient(NodeUnknown, _))::_, _, Choice2Of2(UnknownRecipientException _ as e) -> 
            Failure(Transient(ActorUnknown, e))::faults, retriesPerError
        | Failure(Transient(NodeUnknown, _))::_, _, Choice2Of2(DeliveryException _ as e) -> 
            Failure(Transient(NodeUnresponsive, e))::faults, retriesPerError
        | Failure(Transient(NodeUnknown, _))::_, _, Choice2Of2(:? TimeoutException as e) -> 
            Failure(Transient(ActorUnresponsive, e))::faults, retriesPerError
        | Failure(Transient(NodeUnresponsive, _))::_, _, Choice2Of2(UnknownRecipientException _ as e) -> 
            Failure(Transient(ActorUnknown, e))::faults, retriesPerError
        | Failure(Transient(NodeUnresponsive, _))::_, _, Choice2Of2(CommunicationException _ as e) -> 
            Failure(Transient(NodeLost, e))::faults, retriesPerError
        | Failure(Transient(NodeUnresponsive, _))::_, _, Choice2Of2(:? TimeoutException as e) -> 
            Failure(Transient(ActorUnresponsive, e))::faults, retriesPerError
        | attempts, _, Choice2Of2 e -> raise (RetryFault(e, liftToFaults attempts))
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

        async {
            let! (attempts, _), _ = AsyncFSM.fsm inputGenerator transitionF' isFiniState' initState'

            return match attempts with
                   | (Success r)::_ -> Choice1Of2 r
                   | _ -> Choice2Of2(liftToFaults attempts)
        }

exception FailureException of Fault list * ActorRef

//TODO!!! Do all actorRef post methods
type ReliableActorRef<'T> =
    inherit ActorRef<'T>

    val private retriesPerError: int// = defaultArg retriesPerError 3
    val private retryInterval: int// = defaultArg retryInterval 1500
    val private attemptTimeout: int// = defaultArg attemptTimeout 30000
    val private baseRef: ActorRef<'T>

    new (baseRef : ActorRef<'T>, ?retriesPerError : int, ?retryInterval : int, ?attemptTimeout : int) = {
        inherit ActorRef<'T>(baseRef)
        retriesPerError = defaultArg retriesPerError 3
        retryInterval = defaultArg retryInterval 1500
        attemptTimeout = defaultArg attemptTimeout 300000000
        baseRef = baseRef
    }

    //should be protected; but whatever
    new (info: SerializationInfo, context: StreamingContext) = 
        let baseRef = info.GetValue("underlyingRef", typeof<ActorRef<'T>>) :?> ActorRef<'T>
        {
            inherit ActorRef<'T>(baseRef)
            retriesPerError = info.GetInt32("retriesPerError")
            retryInterval = info.GetInt32("retryInterval")
            attemptTimeout = info.GetInt32("attemptInterval")
            baseRef = baseRef
        }

    member self.UnreliableRef = self.baseRef

    override self.AsyncPost(msg: 'T) : Async<unit> = async {
        let reliablePostExecutor = new RelieablePostExecutor<_>(self.retriesPerError, self.retryInterval, self.attemptTimeout)

        let! result = reliablePostExecutor.ExecPost(self.baseRef.AsyncPost msg)
        return match result with
               | Choice1Of2 r -> r
               | Choice2Of2 faults -> raise <| FailureException(faults, self.baseRef)
    }

    override self.Post(msg : 'T) : unit = 
        let reliablePostExecutor = new RelieablePostExecutor<_>(self.retriesPerError, self.retryInterval, self.attemptTimeout)

        let result = reliablePostExecutor.ExecPost(async { return self.baseRef.Post msg }) |> Async.RunSynchronously
        match result with
        | Choice1Of2 r -> r
        | Choice2Of2 faults -> raise <| FailureException(faults, self.baseRef)

    override self.PostWithReply(msgBuilder : IReplyChannel<'R> -> 'T, ?timeout: int) : Async<'R> = 
        async {
            let reliablePostExecutor = new RelieablePostExecutor<_>(self.retriesPerError, self.retryInterval, self.attemptTimeout)

            let! result = reliablePostExecutor.ExecPost(self.baseRef.PostWithReply(msgBuilder, ?timeout = timeout))
            return match result with
                   | Choice1Of2 r -> r
                   | Choice2Of2 faults -> raise <| FailureException(faults, self.baseRef)
        }

    override self.SerializationDestructor(info: SerializationInfo, context: StreamingContext) =
        info.AddValue("retriesPerError", self.retriesPerError)
        info.AddValue("retryInterval", self.retryInterval)
        info.AddValue("attemptInterval", self.attemptTimeout)
        info.AddValue("underlyingRef", self.baseRef)
        base.SerializationDestructor(info, context)

//    interface ISerializable with
//        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
//            info.AddValue("retriesPerError", self.retriesPerError)
//            info.AddValue("retryInterval", self.retryInterval)
//            info.AddValue("attemptInterval", self.attemptTimeout)
//            info.AddValue("underlyingRef", self.baseRef)

type ReliableActorRef = //TODO!!! Handle the case: ReliableActorRef.FromRef(reliableActorRef)
    static member FromRef(actorRef : ActorRef<'T>, ?retriesPerError : int, ?retryInterval : int, ?attemptTimeout : int) : ReliableActorRef<'T> =
        new ReliableActorRef<_>(actorRef, ?retriesPerError = retriesPerError, ?retryInterval = retryInterval, ?attemptTimeout = attemptTimeout)


