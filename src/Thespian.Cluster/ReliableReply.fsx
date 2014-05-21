//Evaluate in fsi before all else
#r "bin/debug/Nessos.Thespian.dll"
#r "bin/debug/Nessos.Thespian.Remote.dll"
#r "bin/debug/Nessos.Thespian.ClientPack.dll"
#r "bin/debug/Nessos.Thespian.ActorExtensions.dll"
#r "../Nessos.MBrace.Utils/bin/debug/Nessos.MBrace.Utils.dll"

#load "ReliableActorRef.fsx"
#load "BehaviorExtensions.fsx"

open Nessos.Thespian
open Nessos.MBrace.Utils

open ReliableActorRef
open BehaviorExtensions

type FaultType =
    | NodeUnknown
    | RecipientUnknown

type Fault =
    | Transient of FaultType
    | Permanent of FaultType

type Attempt = Success | Failure of Fault

type ReliableReplyExecutor(retriesPerError: int, retryInterval: int) =
    let faultCardinality = 2
    let maxRetries = retriesPerError * faultCardinality

    let initState = [], retriesPerError
    let isFiniState = function (Success _)::_, _ | (Failure _)::_, 0 -> true | _ -> false

    let transitionF state (input : Choice<unit, exn>) = 
        let faults, n = state
        match faults, n, input with
        //final states
        | faults, _, Choice1Of2 _ -> Success::faults, 0
        | Failure(Permanent _)::_, _, _ -> faults, 0
        | _, 0, _ -> faults, 0
        //init state
        | [], n, Choice2Of2(UnknownRecipientException _) -> [Failure(Transient RecipientUnknown)], n - 1
        | [], n, Choice2Of2(CommunicationException _) -> [Failure(Transient NodeUnknown)], n - 1
        //identity states
        | Failure(Transient RecipientUnknown)::_, n, Choice2Of2(UnknownRecipientException _) 
        | Failure(Transient NodeUnknown)::_, n, Choice2Of2(CommunicationException _) -> faults, n - 1
        | _ -> failwith "Retry strategy fault."

    member __.ExecReply(r : unit -> unit) : Async<Choice<unit, Fault list>> =
        let inputGenerator = async {
            let post = async { do r() }

            return! Async.Catch post
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
                   | Success::_ -> Choice1Of2()
                   | _ -> Choice2Of2(liftToFaults attempts)
        }

exception ReplyFailureException of Fault list

let reliableReply (retriesPerError: int) (retryInterval: int) (ctx: BehaviorContext<'T>) (unreliableReply: 'R -> unit) (r: 'R) =
    let reliableReplyExecutor = new ReliableReplyExecutor(retriesPerError, retryInterval)

    let result = reliableReplyExecutor.ExecReply(fun () -> unreliableReply r) |> Async.RunSynchronously
    match result with
    | Choice1Of2() -> ()
    | Choice2Of2 faults ->
        ctx.LogWarning(sprintf "Reply failure: %A" (ReplyFailureException faults))

let (|RR|) (ctx: BehaviorContext<'T>) (replyChannel: IReplyChannel<'R>) =
    reliableReply 2 1000 ctx replyChannel.Reply

let (|RRi|) (ctx: BehaviorContext<'T>) (replyChannel: IReplyChannel<'R>) =
    fun retriesPerError retryInterval r -> reliableReply retriesPerError retryInterval ctx replyChannel.Reply r
