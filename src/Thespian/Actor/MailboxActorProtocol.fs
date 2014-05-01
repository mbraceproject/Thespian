namespace Nessos.Thespian

    open System
    open System.Threading

    [<Serializable>]
    type MailboxActorId(uuid: ActorUUID, name: string) =
        inherit ActorId()

        let idValue = sprintf "%A/%s" uuid (if name = String.Empty then "*" else name)

        override actorId.ToString() = idValue

    type MailboxReplyChannel<'T>(asyncReplyChannel: AsyncReplyChannel<Reply<'T>> option) =
        let mutable timeout = -1
        let mutable asyncRc = asyncReplyChannel

        member rc.Timeout with get() = timeout and set(timeout': int) = timeout <- timeout'
        member rc.WithTimeout(timeout: int) = rc.Timeout <- timeout; rc
        member rc.SetAsyncReplyChannel(asyncReplyChannel) = asyncRc <- asyncReplyChannel

        interface IReplyChannel with
            member rc.Protocol = "mailbox"
            member rc.Timeout with get() = timeout and set(timeout': int) = timeout <- timeout'
            member rc.ReplyUntyped(reply: Reply<obj>) = asyncRc.Value.Reply(match reply with Value v -> Value (v :?> 'T) | Exception e -> Reply<'T>.Exception(e))
        interface IReplyChannel<'T> with 
            member rc.WithTimeout(timeout: int) = rc.WithTimeout(timeout) :> IReplyChannel<'T>
            member rc.Reply(reply: Reply<'T>) = asyncRc.Value.Reply(reply)


    type MailboxProtocolServer<'T>(actorName: string) =
      let mutable cancellationTokenSource: CancellationTokenSource option = None
      let mutable mailboxProcessor: MailboxProcessor<'T> option = None
      let logEvent = new Event<Log>()
      let mutable errorRemover: IDisposable option = None
      let newNotStartedException () =
        new ActorInactiveException("Protocol is stopped. Start actor by calling Actor<'T>.Start()")

      let protocolName = "mailbox"
      //TODO!!! Remove uuid from actorid
      let actorId = new MailboxActorId(Guid.NewGuid(), actorName) :> ActorId

      member __.Mailbox = mailboxProcessor

      abstract Start: (unit -> Async<unit>) -> unit
      default __.Start(body: unit -> Async<unit>) =
        match cancellationTokenSource with
        | Some _ -> ()
        | None ->
          let tokenSource = new CancellationTokenSource()
          cancellationTokenSource <- Some(tokenSource)
          let mailbox = new MailboxProcessor<'T>((fun _ -> body()), tokenSource.Token)
          errorRemover <- Some( mailbox.Error.Subscribe(fun msg -> logEvent.Trigger(Error, Protocol protocolName, new ActorFailedException("Actor behavior unhandled exception.", msg) :> obj)) )
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

      override __.ToString() = sprintf "%s://%O.%s" protocolName actorId typeof<'T>.Name

      interface IPrimaryProtocolServer<'T> with
        override __.ProtocolName = protocolName
        override __.ActorId = actorId
        override __.Log = logEvent.Publish
        override __.PendingMessages
          with get () =
            match mailboxProcessor with
            | Some mailbox -> mailbox.CurrentQueueLength
            | None -> 0
        override __.Start() = invalidOp "Principal protocol; Use the overload that requires the actor body."
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

    type MailboxProtocolClient<'T>(server: MailboxProtocolServer<'T>) =
      let srv = server :> IProtocolServer<'T>

      let protectMailbox f =
        match server.Mailbox with
        | Some mailbox -> f mailbox
        | None ->
          //TODO!! Change this to a communication exception (DeliveryExcpetion)
          //with inner exception ActorInactiveException
          invalidOp "Not started"

      let mailboxPost message (mailbox: MailboxProcessor<_>) = mailbox.Post message
      let post = protectMailbox << mailboxPost

      interface IProtocolClient<'T> with
        override __.ProtocolName = srv.ProtocolName
        override __.ActorId = srv.ActorId
        override __.Post(message: 'T) = post message
        override __.PostAsync(message: 'T) = async { return post message }
        override __.PostWithReply(messageF: IReplyChannel<'R> -> 'T, timeout: int) =
          protectMailbox (fun mailbox ->
            async {
              let! reply = mailbox.PostAndAsyncReply<Reply<'R>>((fun asyncReplyChannel -> messageF <| ReplyChannelProxy<_>(MailboxReplyChannel<_>(Some asyncReplyChannel).WithTimeout(timeout))), timeout)
                    
              return match reply with
                     | Value value -> value
                     | Exception ex -> raise (new MessageHandlingException("Actor threw exception while handling message.", ex))
            })
        override __.TryPostWithReply(messageF: IReplyChannel<'R> -> 'T, timeout: int) =
          protectMailbox (fun mailbox ->
            async {
              let! reply = mailbox.PostAndTryAsyncReply<Reply<'R>>((fun asyncReplyChannel -> messageF(ReplyChannelProxy<_>(MailboxReplyChannel<_>(Some asyncReplyChannel).WithTimeout(timeout)))), timeout)

              return match reply with
                     | Some(Value value) -> Some value
                     | Some(Exception e) -> raise <| new MessageHandlingException("Actor threw exception while handling message.", e)
                     | None -> None
            })

    type MailboxActorProtocol<'T>(actorUUId: ActorUUID, actorName: string) as self =
        let mutable cancellationTokenSource: CancellationTokenSource option = None

        let mutable mailboxProcessor: MailboxProcessor<'T> option = None

        let logEvent = new Event<Log>()
        let mutable errorRemover: IDisposable option = None

        let protocol = self :> IPrincipalActorProtocol<'T>

        let newNotStartedException () =
            new ActorInactiveException("Protocol is stopped. Start actor by calling Actor<'T>.Start()")

        abstract Start: (unit -> Async<unit>) -> unit
        default p.Start(body: unit -> Async<unit>) = 
            match cancellationTokenSource with
            | Some _ -> 
                ()
            | None ->
                let tokenSource = new CancellationTokenSource()
                cancellationTokenSource <- Some(tokenSource)
                let mailbox = new MailboxProcessor<'T>((fun _ -> body()), tokenSource.Token)
                errorRemover <- Some( mailbox.Error.Subscribe(fun msg -> logEvent.Trigger(Error, Protocol protocol.ProtocolName, new ActorFailedException("Actor behavior unhandled exception.", msg) :> obj)) )
                mailboxProcessor <- Some mailbox
                mailbox.Start()

        member p.Stop() = 
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

        override p.ToString() =
            sprintf "%s://%O.%s" 
                protocol.ProtocolName 
                protocol.ActorId
                typeof<'T>.Name

        interface IPrincipalActorProtocol<'T> with

            member p.Configuration = None 

            member p.ProtocolName = "mailbox"

            member p.ActorUUId = actorUUId

            member p.ActorName = actorName

            member p.ActorId = new MailboxActorId(actorUUId, actorName) :> ActorId

            member p.MessageType = typeof<'T>

            member p.Log = logEvent.Publish

            member p.CurrentQueueLength 
                with get() =
                    match mailboxProcessor with
                    | Some mailbox -> mailbox.CurrentQueueLength
                    | None -> 0

            member p.Start() = 
                raise <| new InvalidOperationException("Principal Actor; Use the overload that requires the actor body.")

            member p.Start(body: unit -> Async<unit>) = self.Start(body)

            member p.Stop() = 
                self.Stop()

            member p.Receive(timeout: int) = 
                match mailboxProcessor with
                | Some mailbox -> 
                    mailbox.Receive(timeout)
                | None -> 
                    raise <| newNotStartedException()

            member p.Receive() = protocol.Receive(-1)

            member p.TryReceive(timeout: int) =
                match mailboxProcessor with
                | Some mailbox -> 
                    mailbox.TryReceive(timeout)
                | None -> raise <| newNotStartedException()

            member p.TryReceive() = protocol.TryReceive(-1)

            member p.Scan(scanner: 'T -> (Async<'U> option), timeout: int) =
                match mailboxProcessor with
                | Some mailbox ->
                    mailbox.Scan(scanner, timeout)
                | None -> raise <| newNotStartedException()

            member p.Scan(scanner: 'T -> (Async<'U> option)) = protocol.Scan(scanner, -1)

            member p.TryScan(scanner: 'T -> (Async<'U> option), timeout: int) =
                match mailboxProcessor with
                | Some mailbox ->
                    mailbox.TryScan(scanner, timeout)
                | None -> raise <| newNotStartedException()

            member p.TryScan(scanner: 'T -> (Async<'U> option)) =
                protocol.TryScan(scanner, -1)

            member p.Post(msg: 'T) = 
                match mailboxProcessor with
                | Some mailbox ->
                    mailbox.Post(msg)
                | None -> 
                raise <| newNotStartedException()

            member p.PostAsync(msg: 'T): Async<unit> =
                async { return protocol.Post(msg) }

            member p.TryPostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, timeout: int): Async<'R option> =
                match mailboxProcessor with
                | Some mailbox ->
                    async {
                        let! reply = mailbox.PostAndTryAsyncReply<Reply<'R>>((fun asyncReplyChannel -> msgBuilder(ReplyChannelProxy<_>(MailboxReplyChannel<_>(Some asyncReplyChannel).WithTimeout(timeout)))), timeout)

                        return
                            match reply with
                            | Some(Value value) -> Some value
                            | Some(Exception e) -> raise <| new MessageHandlingException("Actor threw exception while handling message.", e)
                            | None -> None
                    }
                | None -> raise <| newNotStartedException()

            member p.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, timeout: int) : Async<'R> = 
                match mailboxProcessor with
                | Some mailbox ->
                    async {
                        let! reply = mailbox.PostAndAsyncReply<Reply<'R>>((fun asyncReplyChannel -> msgBuilder <| ReplyChannelProxy<_>(MailboxReplyChannel<_>(Some asyncReplyChannel).WithTimeout(timeout))), timeout)
                    
                        return
                            match reply with
                            | Value value -> value
                            | Exception ex -> raise (new MessageHandlingException("Actor threw exception while handling message.", ex))
                    }
                | None -> raise <| newNotStartedException()

            member p.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T) : Async<'R> =
                match mailboxProcessor with
                | Some mailbox ->
                    async {
                        let mailboxChannel = new MailboxReplyChannel<_>(None)
                        let replyChannel = new ReplyChannelProxy<_>(mailboxChannel)
                        let message = msgBuilder replyChannel

                        let msgBuilder' asyncReplyChannel =
                            mailboxChannel.SetAsyncReplyChannel(Some asyncReplyChannel)
                            message

                        let! reply = mailbox.PostAndAsyncReply<Reply<'R>>(msgBuilder', mailboxChannel.Timeout)
                    
                        return
                            match reply with
                            | Value value -> value
                            | Exception ex -> raise (new MessageHandlingException("Actor threw exception while handling message.", ex))
                    }
                | None -> raise <| newNotStartedException()
