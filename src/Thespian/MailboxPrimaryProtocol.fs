module Nessos.Thespian.Mailbox

open System
open System.Threading

open Nessos.Thespian.Logging

let ProtocolName = "mailbox"

[<Serializable>]
type MailboxActorId(name : string) = 
    inherit ActorId(name)
    override actorId.ToString() = name

type MailboxReplyChannel<'T>(asyncReplyChannel: AsyncReplyChannel<Reply<'T>> option) =
    let mutable timeout = Default.ReplyReceiveTimeout
    let mutable asyncRc = asyncReplyChannel
    
    member __.Timeout with get() = timeout and set(timeout': int) = timeout <- timeout'
    member self.WithTimeout(timeout: int) = self.Timeout <- timeout; self
    //TODO consider making internal
    member  __.SetAsyncReplyChannel(asyncReplyChannel) = asyncRc <- asyncReplyChannel
    
    member __.ReplyUntyped(reply: Reply<obj>) = asyncRc.Value.Reply(Reply.unbox<'T> reply)
    member __.Reply(reply: Reply<'T>) = asyncRc.Value.Reply(reply)
    
    interface IReplyChannel<'T> with
        override self.Protocol = ProtocolName
        override self.Timeout with get() = timeout and set(timeout': int) = timeout <- timeout'
        override self.AsyncReplyUntyped(reply: Reply<obj>) = async.Return <| self.ReplyUntyped(reply)
        override self.AsyncReply(reply: Reply<'T>) = async.Return <| self.Reply(reply)    
    
//    interface IReplyChannel<'T> with 
//        override self.WithTimeout(timeout: int) = self.WithTimeout(timeout) :> IReplyChannel<'T>
//        override self.Reply(reply: Reply<'T>) = self.Reply(reply)
        


type MailboxProtocolServer<'T>(actorName: string) =
    let mutable cancellationTokenSource: CancellationTokenSource option = None
    let mutable mailboxProcessor: MailboxProcessor<'T> option = None
    let logEvent = new Event<Log>()
    let mutable errorRemover: IDisposable option = None
    let newNotStartedException () = new ActorInactiveException("Protocol is stopped. Start actor by calling Actor<'T>.Start()")

    let actorId = new MailboxActorId(actorName) :> ActorId

    member __.Mailbox = mailboxProcessor

    abstract Start: (unit -> Async<unit>) -> unit
    default __.Start(body: unit -> Async<unit>) =
        match cancellationTokenSource with
        | Some _ -> ()
        | None ->
            let tokenSource = new CancellationTokenSource()
            cancellationTokenSource <- Some(tokenSource)
            let mailbox = new MailboxProcessor<'T>((fun _ -> body()), tokenSource.Token)
            errorRemover <- Some( mailbox.Error.Subscribe(fun msg -> logEvent.Trigger(Error, Protocol ProtocolName, new ActorFailedException("Actor behavior unhandled exception.", msg) :> obj)) )
            mailboxProcessor <- Some mailbox
            mailbox.Start()

    member __.Stop() =
        match cancellationTokenSource with
        | Some tokenSource ->
            tokenSource.Cancel()
            match mailboxProcessor with
            | Some mailboxProcessor ->
                (mailboxProcessor :> IDisposable).Dispose()
            | None _ -> ()
            cancellationTokenSource <- None
            mailboxProcessor <- None
            match errorRemover with
            | Some remover -> remover.Dispose(); errorRemover <- None
            | _ -> ()
        | None -> ()

    override __.ToString() = sprintf "%s://%O.%s" ProtocolName actorId typeof<'T>.Name

    interface IPrimaryProtocolServer<'T> with 
        override __.ProtocolName = ProtocolName
        override __.ActorId = actorId
        override self.Client = new MailboxProtocolClient<'T>(self) :> IProtocolClient<'T>
        override __.Log = logEvent.Publish
        override __.PendingMessages
            with get () =
                match mailboxProcessor with
                | Some mailbox -> mailbox.CurrentQueueLength
                | None -> 0
        override __.Start() = invalidOp "Principal protocol; use the overload that requires the actor body."
        override self.Stop() = self.Stop()
        
        override __.Receive(timeout: int) =
            match mailboxProcessor with
            | Some mailbox -> mailbox.Receive(timeout)
            | None -> raise <| newNotStartedException()
        
        override __.TryReceive(timeout: int) =
            match mailboxProcessor with
            | Some mailbox -> mailbox.TryReceive(timeout)
            | None -> raise <| newNotStartedException()
        
        override self.Start(body: unit -> Async<unit>) = self.Start(body)
        override self.Dispose() = self.Stop()


and MailboxProtocolClient<'T>(server: MailboxProtocolServer<'T>) =
    let srv = server :> IProtocolServer<'T>

    let protectMailbox f =
        match server.Mailbox with
        | Some mailbox -> f mailbox
        | None -> raise <| new ActorInactiveException("The target actor is stopped.", srv.ActorId)

    let mailboxPost message (mailbox: MailboxProcessor<_>) = mailbox.Post message
    let post = protectMailbox << mailboxPost

    override __.ToString() = let ub = new UriBuilder(ProtocolName, srv.ActorId.Name) in ub.Uri.ToString()

    interface IProtocolClient<'T> with
        override __.ProtocolName = srv.ProtocolName
        override __.Factory = None
        override __.ActorId = srv.ActorId
        override __.Uri = String.Empty
        override __.Post(message: 'T) = post message
        override __.AsyncPost(message: 'T) = async { return post message }
        override __.PostWithReply(messageF: IReplyChannel<'R> -> 'T, timeout: int) =
            protectMailbox (fun mailbox ->
                async {
                    //we run the message constructor with an empty channel at first
                    //so that we can get any timeout override
                    let mailboxChannel = new MailboxReplyChannel<_>(None)
                    let initTimeout = mailboxChannel.Timeout
                    let replyChannel = new ReplyChannelProxy<_>(mailboxChannel)
                    let message = messageF replyChannel

                    let msgF asyncReplyChannel =
                      mailboxChannel.SetAsyncReplyChannel(Some asyncReplyChannel)
                      message

                    let timeout' = if initTimeout <> mailboxChannel.Timeout then mailboxChannel.Timeout else timeout
                    
                    let! reply = mailbox.PostAndAsyncReply<Reply<'R>>(msgF, timeout')
                              
                    return 
                        match reply with
                        | Value value -> value
                        | Exn ex -> raise (new MessageHandlingException("Actor threw exception while handling message.", srv.ActorId, ex))
                })
        override __.TryPostWithReply(messageF: IReplyChannel<'R> -> 'T, timeout: int) =
            protectMailbox (fun mailbox ->
                async {
                  //we run the message constructor with an empty channel at first
                  //so that we can get any timeout override
                  let mailboxChannel = new MailboxReplyChannel<_>(None)
                  let initTimeout = mailboxChannel.Timeout
                  let replyChannel = new ReplyChannelProxy<_>(mailboxChannel)
                  let message = messageF replyChannel

                  let msgF asyncReplyChannel =
                    mailboxChannel.SetAsyncReplyChannel(Some asyncReplyChannel)
                    message

                  let timeout' = if initTimeout <> mailboxChannel.Timeout then mailboxChannel.Timeout else timeout
                  
                  let! reply = mailbox.PostAndTryAsyncReply<Reply<'R>>(msgF, timeout')

                  return 
                    match reply with
                    | Some(Value value) -> Some value
                    | Some(Exn e) -> raise <| new MessageHandlingException("Actor threw exception while handling message.", srv.ActorId, e)
                    | None -> None
                })
