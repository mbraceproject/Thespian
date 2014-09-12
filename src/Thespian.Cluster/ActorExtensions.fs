[<AutoOpen>]
module Nessos.Thespian.Cluster.ActorExtensions
    
open System

open Nessos.Thespian
open Nessos.Thespian.Tools

exception BroadcastPartialFailureException of string * (ActorRef * exn) list
exception BroadcastFailureException of string

exception FailoverFailureException of string

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
//        targets |> action (fun target -> async { do target <-- msg })
        targets |> action (fun target -> async { do! target <-!- msg })

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

module Failover =
    let rec private failoverLoop post targets = async {
        match targets with
        | target::targets' ->
            try
                return! post target
            with FailureException _ ->
                return! failoverLoop post targets'
        | [] ->
            return! Async.Raise (FailoverFailureException "All failovers have failed.")
    }

    let postWithReply (msgF: IReplyChannel<'R> -> 'T) (targets: #seq<#ActorRef<'T>>): Async<'R> =
        targets |> Seq.toList |> failoverLoop (fun target -> target <!- msgF)

module ActorRef =
    let toUniTcpAddress (actorRef: ActorRef<'T>) =
        let addresses = actorRef.GetUris()
                        |> List.map (fun uri -> new Uri(uri))
                        |> List.choose (fun uri -> if uri.Scheme = Remote.TcpProtocol.Unidirectional.ProtocolName || uri.Scheme = Remote.TcpProtocol.Bidirectional.ProtocolName then Some(new Remote.TcpProtocol.Address(uri.Host, uri.Port)) else None)
        match addresses with
        | [] -> None
        | addr::_ -> Some addr
        

module Actor =
    type private LogSubscribedActor<'T> internal (otherActor: Actor<'T>, observerF: Log -> unit) =
        inherit Actor<'T>(otherActor)

        let mutable subscriptionRef = None : IDisposable option 

        override self.Publish(protocols: IProtocolServer<'T>[]) = 
            new LogSubscribedActor<'T>(otherActor.Publish(protocols), observerF) :> Actor<'T>

        override self.Publish(configurations: #seq<'U> when 'U :> IProtocolFactory) =
            new LogSubscribedActor<'T>(otherActor.Publish(configurations), observerF) :> Actor<'T>

        override self.Rename(newName: string) =
            new LogSubscribedActor<'T>(otherActor.Rename(newName), observerF) :> Actor<'T>

        override self.Start() =
            subscriptionRef |> Option.iter (fun d -> d.Dispose())
            subscriptionRef <- self.Log |> Observable.subscribe observerF |> Some
            base.Start()

        override self.Stop() =
            subscriptionRef |> Option.iter (fun d -> d.Dispose())
            base.Stop()


    let subscribeLog (observerF: Log -> unit) (actor: Actor<'T>): Actor<'T> = 
        new LogSubscribedActor<'T>(actor, observerF) :> Actor<'T>

