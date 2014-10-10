module Nessos.Thespian.Observable

open System
open System.Threading

open Nessos.Thespian

type ObservableProtocolClient<'T>(actorName : string, observableActorRef : ActorRef<'T>) =
    override __.ToString() = let ub = new UriBuilder("observable", actorName) in ub.Uri.ToString()

    interface IProtocolClient<'T> with
        override __.ProtocolName = "observable"
        override __.ActorId = new MailboxProtocol.MailboxActorId(actorName) :> ActorId
        override __.Uri = String.Empty
        override __.Factory = None
        override __.Post(msg : 'T) = observableActorRef.Post(msg)
        override __.AsyncPost(msg : 'T) = observableActorRef.AsyncPost(msg)
        override __.PostWithReply<'R>(msgF : IReplyChannel<'R> -> 'T, timeout : int) = 
            observableActorRef.PostWithReply(msgF, timeout)
        override __.TryPostWithReply<'R>(msgF : IReplyChannel<'R> -> 'T, timeout : int) = 
            observableActorRef.TryPostWithReply(msgF, timeout)

and ObservableProtocolServer<'T>(actorName : string, observable : IObservable<'T>) = 
    let observationActor = 
        new Actor<'T>(actorName, [| new MailboxProtocol.MailboxProtocolServer<'T>(actorName) :> IProtocolServer<'T> |], 
                      fun _ -> async.Zero())
    let actorId = new MailboxProtocol.MailboxActorId(actorName)
    let client = new ObservableProtocolClient<'T>(actorName, observationActor.Ref)

    [<VolatileField>]
    let mutable remover : IDisposable option = None

    [<VolatileField>]
    let mutable cancellationTokenSource = new CancellationTokenSource()

    member __.Start(body : unit -> Async<unit>) = 
        match remover with
        | None -> 
            observationActor.Start()
            Async.Start(body(), cancellationTokenSource.Token)
            remover <- Some <| observable.Subscribe observationActor.Ref.Post
        | Some _ -> ()

    member __.Stop() = 
        match remover with
        | Some disposable -> 
            observationActor.Stop()
            cancellationTokenSource.Cancel()
            cancellationTokenSource <- new CancellationTokenSource()
            disposable.Dispose()
            remover <- None
        | None -> ()

    interface IPrimaryProtocolServer<'T> with
        override __.ProtocolName = "observable"
        override __.ActorId = actorId :> ActorId
        override __.Client = client :> IProtocolClient<'T>
        override __.Log = observationActor.Log
        override __.Start() = invalidOp "Principal protocol; use the overload tha requires actor body."
        override self.Stop() = self.Stop()
        override __.CreateInstance(actorName: string) = new ObservableProtocolServer<'T>(actorName, observable) :> IPrimaryProtocolServer<'T>
        override __.PendingMessages = observationActor.PendingMessages
        override __.Receive(timeout : int) = observationActor.Receive(timeout)
        override __.TryReceive(timeout : int) = observationActor.TryReceive(timeout)
        override self.Start(body : unit -> Async<unit>) = self.Start(body)
        override __.Dispose() = __.Stop()


