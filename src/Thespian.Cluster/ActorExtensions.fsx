//Evaluate in fsi before all else
#r "bin/debug/Nessos.Thespian.dll"
#r "bin/debug/Nessos.Thespian.Remote.dll"
#r "bin/debug/Nessos.Thespian.ClientPack.dll"
#r "bin/debug/Nessos.Thespian.ActorExtensions.dll"
#r "../Nessos.MBrace.Utils/bin/debug/Nessos.MBrace.Utils.dll"

open Nessos.Thespian
open Nessos.MBrace.Utils

exception BroadcastPartialFailureException of string * (ActorRef * exn) list
exception BroadcastFailureException of string

type Broadcast<'T, 'R> = {
    ThrowOnEmptyTargets: bool
    Targets: seq<ActorRef<'T>>
    Post: ActorRef<'T> -> Async<Choice<'R, ActorRef<'T> * exn>>
    Process: 'R[] * (ActorRef<'T> * exn) [] -> 'R[] * (ActorRef<'T> * exn) []
}

module Broadcast =

    let internal wrapPost postF actorRef = async {
        let! c = Async.Catch (postF actorRef)
        return match c with
               | Choice1Of2 r -> Choice1Of2 r
               | Choice2Of2 e -> Choice2Of2(actorRef, e)
    }

    let action (doF: ActorRef<'T> -> Async<'R>) (targets: #seq<#ActorRef<'T>>) = {
        ThrowOnEmptyTargets = false
        Targets = targets |> Seq.cast |> Seq.cache
        Post = wrapPost doF
        Process = id
    }

    let post (msg: 'T) (targets: #seq<#ActorRef<'T>>) = 
        targets |> action (fun target -> async { do target <-- msg })

    let postWithReply (msgF: IReplyChannel<'R> -> 'T) (targets: #seq<#ActorRef<'T>>) = 
        targets |> action (fun target -> target <!- msgF)

    let ignoreFaults (exnFilter: exn -> bool) (broadcast: Broadcast<'T, 'R>) = 
        { broadcast with
            Process = fun (rs, exns) -> rs, exns |> Array.choose (function _, e when exnFilter e -> None | e -> Some e)
        }

    let onFault (exnF: ActorRef<'T> * exn -> unit) (broadcast: Broadcast<'T, 'R>) =
        { broadcast with
            Process = fun (rs, exns) -> exns |> Seq.iter exnF; rs, exns
        }

    let allFaults = fun (_ : exn) -> true

    let tryExec (broadcast: Broadcast<'T, 'R>) = async {
        if broadcast.ThrowOnEmptyTargets && (Seq.isEmpty broadcast.Targets) then
            return Choice2Of2 (BroadcastFailureException "No targets available")
        else
            let! cs = broadcast.Targets |> Seq.map broadcast.Post |> Async.Parallel
            return Choice1Of2 (Choice.splitArray cs |> broadcast.Process)
    }

    let exec (broadcast: Broadcast<'T, 'R>) = async {
        let! c = tryExec broadcast
        match c with
        | Choice1Of2(rs, exns) ->
            if Array.isEmpty exns then return rs
            else if Array.isEmpty rs then
                return! Async.Raise <| BroadcastFailureException("Total broadcast failure.")
            else return! Async.Raise <| BroadcastPartialFailureException("Partial failures in brodcast.", exns |> Seq.map (fun (r, e) -> r :> ActorRef, e) |> Seq.toList)
        | Choice2Of2 e ->
            return! Async.Raise e
    }
    

open System
open System.Reflection

type Record = {
    Integer: int
    Str: string
} with static member Default = { Integer = 0; Str = String.Empty }

typeof<Record>.GetProperties()
typeof<Record>.GetProperties(BindingFlags.Instance|||BindingFlags.Public)
