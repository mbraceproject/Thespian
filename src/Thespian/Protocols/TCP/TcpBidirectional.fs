module Nessos.Thespian.Remote.TcpProtocol.Bidirectional

open System
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open System.Collections.Concurrent

open Nessos.Thespian
open Nessos.Thespian.Utils.Async
open Nessos.Thespian.Utils.Task
open Nessos.Thespian.Logging
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol.ConnectionPool

let ProtocolName = "btcp"

let internal attempt = TcpProtocol.Unidirectional.attempt
let internal addressToEndPoints = TcpProtocol.Unidirectional.addressToEndpoints
        
type ProtocolMessageStream(protocolStream: ProtocolStream) =
    let refCount = ref 0
    let serializer = Serialization.defaultSerializer
    let disposeTcs = new TaskCompletionSource<bool>()
  
    member __.ProtocolStream = protocolStream

    member __.AsyncWriteProtocolMessage(msgId: MsgId, actorId: TcpActorId, protocolMessage: 'T, ?serializationContext: MessageSerializationContext) =
        async {
            let context = serializationContext |> Option.map (fun c -> c.GetStreamingContext())
            let serializedProtocolMessage = serializer.Serialize<obj>(protocolMessage, ?context = context)
            let protocolRequest = msgId, actorId, serializedProtocolMessage : ProtocolRequest
            return! protocolStream.AsyncWriteRequest(protocolRequest)
        }

    member __.TryAsyncWriteProtocolMessage(msgId: MsgId, actorId: TcpActorId, protocolMessage: 'T, ?serializationContext: MessageSerializationContext) =
        async {
            let context = serializationContext |> Option.map (fun c -> c.GetStreamingContext())
            let serializedProtocolMessage = serializer.Serialize<obj>(protocolMessage, ?context = context)
            let protocolRequest = msgId, actorId, serializedProtocolMessage : ProtocolRequest
            return! protocolStream.TryAsyncWriteRequest(protocolRequest)
        }

    member self.Acquire() = Interlocked.Increment(refCount) |> ignore; self
    member __.Dispose() = if Interlocked.Decrement(refCount) = 0 then disposeTcs.SetResult(true)
    member __.AsyncWaitForDisposal(timeout: int) =
        if refCount.Value = 0 then async.Return (Some true)
        else disposeTcs.Task.TimeoutAfter(timeout) |> Async.AwaitTask

    interface IDisposable with override self.Dispose() = self.Dispose()


type AsyncExecutor =
    | Exec of IReplyChannel<unit> * Async<unit>
    | StartExecs of IReplyChannel<unit>

let private asyncExecutorBehavior =
    let rec behavior (isStarted: bool,  pending: AsyncExecutor list) (m: AsyncExecutor) =
        async {
            match m with
            | Exec(rc, computation) when isStarted ->
                try
                    do! computation //perform computation
                    do! rc.Reply () //say when finished
                with e -> //something went wrong with the reply
                    do! rc.ReplyWithException e

                return isStarted, pending

            | Exec _ -> return isStarted, m::pending
            | StartExecs rc when isStarted -> 
                do! rc.Reply () 
                return isStarted, pending

            | StartExecs rc ->
                let pendingRev = List.rev pending
                let! s' = pendingRev |> List.foldAsync behavior (true, [])
                do! rc.Reply ()
                return s'
        }

    Behavior.stateful (false, []) behavior


type DeserializationContext(stream: ProtocolMessageStream) =
    inherit TcpProtocol.Unidirectional.DeserializationContext()
    let asyncExecutor = Actor.bind asyncExecutorBehavior |> Actor.start

    member __.ProtocolMessageStream = stream
    member __.ReplyWriter = asyncExecutor.Ref
    member self.GetStreamingContext() = new StreamingContext(StreamingContextStates.All, self)
    interface IDisposable with override __.Dispose() = asyncExecutor.Stop()
  

type ReplyChannel = TcpProtocol.Unidirectional.ReplyChannel

[<Serializable>]
type ReplyChannel<'T> =
    inherit ReplyChannel
        
    val private stream: ProtocolMessageStream
    val private replyWriter: ActorRef<AsyncExecutor>
    val private isOverride: bool
            
    new (actorId: TcpActorId, msgId: MsgId, ?isOverride: bool) =
        let isOverride = defaultArg isOverride false
        {
            inherit ReplyChannel(actorId, msgId)
            //stream is set to null because this constructor is only used at client side
            stream = Unchecked.defaultof<ProtocolMessageStream>
            replyWriter = Unchecked.defaultof<ActorRef<AsyncExecutor>>
            isOverride = isOverride
        }

    internal new (info: SerializationInfo, context: StreamingContext) =
        let stream, replyWriter = 
            match context.Context with
            | :? DeserializationContext as deserializationContext ->
                deserializationContext.ProtocolMessageStream.Acquire(), deserializationContext.ReplyWriter
            | _ -> invalidArg "context" "Invalid deserialization context given."
        {
            inherit ReplyChannel(info, context)
            stream = stream
            replyWriter = replyWriter
            isOverride = false
        }

    member self.IsOverride = self.isOverride

    member self.ProtocolMessageStream =
        if self.stream <> Unchecked.defaultof<ProtocolMessageStream> then self.stream.ProtocolStream
        else invalidOp "ProtocolMessageStream is not accessible from client side code."

    member self.ReplyWriter = 
        if self.replyWriter <> Unchecked.defaultof<ActorRef<AsyncExecutor>> then self.replyWriter
        else invalidOp "ReplyWriter is not accessible from client side code."

    member private self.AsyncReplyOp(reply: Reply<'T>) =
        async {
            try
                use _ = self.stream

                let! r = self.stream.TryAsyncWriteProtocolMessage(self.MessageId, self.ActorId, Reply.box reply)
                match r with
                | Some() ->
                    let! r' = self.stream.ProtocolStream.TryAsyncReadResponse()
                    match r' with
                    | Some(Acknowledge _) -> return ()
                    | Some(UnknownRecipient _) -> return! Async.Raise <| new UnknownRecipientException("Reply recipient not found on the other end of the reply channel.", self.ActorId)
                    | Some(Failure(_, e)) -> return! Async.Raise <| new DeliveryException("Failure occurred on the other end of the reply channel.", self.ActorId, e)
                    | None -> return! Async.Raise <| new CommunicationTimeoutException("Timeout occurred while waiting for confirmation of reply delivery.", self.ActorId, TimeoutType.ConfirmationRead)

                | None -> return! Async.Raise <| new CommunicationTimeoutException("Timeout occurred while trying to write reply on channel.", self.ActorId, TimeoutType.MessageWrite)

            with 
            | :? CommunicationException as e -> return! Async.Raise e
            | e -> return! Async.Raise <| new CommunicationException("Failure occurred while trying to reply.", self.ActorId, e)
        }

    member self.AsyncReply(reply: Reply<'T>) = async {
        try do! self.replyWriter <!- fun ch -> Exec(ch.WithTimeout(Timeout.Infinite), self.AsyncReplyOp(reply))
        with 
        | MessageHandlingException(_, e) -> return! Async.Raise e
        | :? ActorInactiveException -> return! Async.Raise <| new CommunicationException("Attempting to reply a second time on a closed channel.", self.ActorId)
        | :? CommunicationException as e -> return! Async.Raise e
        | e -> return! Async.Raise <| new CommunicationException("Failure occurred while trying to reply.", self.ActorId, e)
    }

    member self.Reply(reply: Reply<'T>) = Async.RunSynchronously <| self.AsyncReply(reply)
    member self.AsyncReplyUtyped(reply: Reply<obj>) = self.AsyncReply(Reply.unbox reply)
    member self.ReplyUntyped(reply: Reply<obj>) = self.Reply(Reply.unbox reply)

    interface IReplyChannel<'T> with
        override self.Protocol = ProtocolName
        override self.Timeout with get() = self.Timeout and set(timeout': int) = self.Timeout <- timeout'    
        override self.AsyncReplyUntyped(reply: Reply<obj>): Async<unit> = self.AsyncReplyUtyped(reply)
        override self.AsyncReply(reply: Reply<'T>) = self.AsyncReply(reply)

    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)


type ReplyResultsRegistry() =
    let results = new ConcurrentDictionary<MsgId, TaskCompletionSource<Choice<Reply<obj>, exn>>>()

    member self.RegisterAndWaitForResponse(msgId: MsgId, timeout: int) =
        let tcs = new TaskCompletionSource<_>()
        results.TryAdd(msgId, tcs) |> ignore
        async {
            try return! Async.AwaitTask(tcs.Task, timeout)
            with _ -> return None
        }

    member __.Unregister(msgId: MsgId) =
        let isValid, tcs = results.TryRemove(msgId)
        if isValid then tcs.TrySetCanceled() |> ignore

    member __.TrySetResult(msgId: MsgId, value: Choice<Reply<obj>, exn>) =
        let isValid, tcs = results.TryRemove(msgId)
        if isValid then tcs.TrySetResult(value) |> ignore
        isValid


type ProtocolClient<'T>(actorId: TcpActorId) =
    let address = actorId.Address
    let serializer = Serialization.defaultSerializer
    let foreignFilter nativeMsgId (rc: IReplyChannel) =
        match rc with
        | :? ReplyChannel as frc -> frc.MessageId <> nativeMsgId
        | _ -> rc.Protocol <> ProtocolName

    let serializationContext nativeMsgId =
        new MessageSerializationContext(serializer,
            {
                new IReplyChannelFactory with 
                    override __.Protocol = ProtocolName
                    override __.IsForeignChannel(rc: IReplyChannel<'U>) =
                        if rc.Protocol = ProtocolName then
                            match rc with
                            | :? ReplyChannel<'U> as rc' -> rc'.MessageId <> nativeMsgId && not rc'.IsOverride
                            | _ -> false
                        else true

                    override __.Create<'R>() = 
                        let newMsgId = MsgId.NewGuid()
                        new ReplyChannelProxy<'R>(new ReplyChannel<'R>(actorId, newMsgId, true))
            })

    let replyRegistry = new ReplyResultsRegistry()
    let logEvent = new Event<Log>()
    let factory = new BTcpFactory(ProtocolMode.Client address)
    let uri = let ub = new UriBuilder(ProtocolName, address.HostnameOrAddress, address.Port, actorId.Name) in ub.Uri.ToString()

    let processReply (protocolStream: ProtocolStream) =
        async {
            let! r = protocolStream.TryAsyncReadRequest()
            match r with
            | Some(replyMsgId, actorId, payload) ->
                try
                    let reply = serializer.Deserialize<obj>(payload) :?> Reply<obj>
                    if not <| replyRegistry.TrySetResult(replyMsgId, Choice1Of2 reply) then
                        let! r' = protocolStream.TryAsyncWriteResponse(UnknownRecipient(replyMsgId, actorId))
                        match r' with
                        | Some() -> return ()
                        | None -> return! Async.Raise <| new CommunicationTimeoutException("Timeout occurred while trying to write a response for received reply.", actorId, TimeoutType.ConfirmationWrite)
                    else
                        let! r'' = protocolStream.TryAsyncWriteResponse(Acknowledge(replyMsgId))
                        match r'' with
                        | Some() -> return ()
                        | None -> return! Async.Raise <| new CommunicationTimeoutException("Timeout occurred while trying to write a response for received reply.", actorId, TimeoutType.ConfirmationWrite)
                with e ->
                    let! r''' = protocolStream.TryAsyncWriteResponse(Failure(replyMsgId, e))
                    match r''' with
                    | Some() -> return ()
                    | None -> return! Async.Raise <| new CommunicationTimeoutException("Timeout occurred while trying to write a response for received reply.", actorId, TimeoutType.ConfirmationWrite)

            | None -> return! Async.Raise <| new CommunicationTimeoutException("Timeout occurred while waiting for a reply message.", actorId, TimeoutType.MessageRead)
        }

    let handleForeignReplyChannel (foreignRc: IReplyChannel, nativeRc: IReplyChannel) =
        //register async wait handle for the nativeRc
        let nativeRcImpl = nativeRc :?> ReplyChannel
        let awaitResponse = replyRegistry.RegisterAndWaitForResponse(nativeRcImpl.MessageId, foreignRc.Timeout)
        async {
            //wait for the native rc result
            let! response = awaitResponse
            match response with
            | Some reply ->
                //forward to foreign rc
                try do! foreignRc.AsyncReplyUntyped <| match reply with Choice1Of2 r -> r | Choice2Of2 e -> Exn e
                with e -> logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to forward a reply.", e) |> box)
            | None -> () //timeout on nativeRc, no need to do anything, timeout will eventually occur on the other side as well
        }

    let setupForeignReplyChannelHandler (context: MessageSerializationContext) =
        if context.ReplyChannelOverrides.Length = 0 then async.Zero()
        else
            context.ReplyChannelOverrides
            |> List.map handleForeignReplyChannel
            |> Async.Parallel
            |> Async.Ignore

    let cancelForeignReplyChannels (context: MessageSerializationContext) =
        context.ReplyChannelOverrides
        |> List.iter (fun (_, nrc) -> let msgId = (nrc :?> ReplyChannel).MessageId in replyRegistry.Unregister(msgId))

    let failForeignReplyChannels (context: MessageSerializationContext) (e: exn) =
        context.ReplyChannelOverrides
        |> List.iter (fun (_, nrc) -> let msgId = (nrc :?> ReplyChannel).MessageId in replyRegistry.TrySetResult(msgId, Choice2Of2 e) |> ignore)

    let failProcessReplies msgId context withReply e =
        //    printfn "Reply process failure: %A" e
        logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, box e)
        failForeignReplyChannels context e
        if withReply then replyRegistry.TrySetResult(msgId, Choice2Of2 e) |> ignore

    let postOnEndpoint targetEndPoint msgId msg withReply =
        async {
            let! connection = TcpConnectionPool.AsyncAcquireConnection targetEndPoint
            try
                let protocolStream = new ProtocolStream(msgId, connection.GetStream())

                //serialize message with the reply patching serialization context
                let context = serializationContext msgId
                let serializedMessage = serializer.Serialize<obj>(msg, context.GetStreamingContext())

                let foreignRcHandle = setupForeignReplyChannelHandler context

                let protocolRequest = msgId, actorId, serializedMessage : ProtocolRequest
                try
                    let! r = protocolStream.TryAsyncWriteRequest(protocolRequest)
                    match r with
                    | Some() ->
                        Async.Start foreignRcHandle

                        let! r'' = async {
                            try return! protocolStream.TryAsyncReadResponse()
                            with :? ThespianSerializationException as e -> return! Async.Raise (new CommunicationException("Unable to verify message delivery.", actorId, e))
                        }
                        match r'' with
                        | Some(Acknowledge _) ->
                            //start reply processing
                            let expectedReplies = context.ReplyChannelOverrides.Length + if withReply then 1 else 0
                            async {
                                use _ = connection
                                use _ = protocolStream
                                try
                                    for _ in 1..expectedReplies do
                                        do! processReply protocolStream

                                with
                                | CommunicationException _ as e -> failProcessReplies msgId context withReply e
                                | e -> failProcessReplies msgId context withReply (new CommunicationException("Failure occurred while posting message.", actorId, e))
                            } |> Async.Start

                        | Some(UnknownRecipient _) -> return! Async.Raise <| new UnknownRecipientException("Remote host could not find the message recipient.", actorId)
                        | Some(Failure(_, e)) -> return! Async.Raise <| new DeliveryException("Message delivery failed on the remote host.", actorId, e)
                        | None -> return! Async.Raise <| new CommunicationTimeoutException("Timeout occurred while waiting for confirmation of message send.", actorId, TimeoutType.ConfirmationRead)

                    | None -> return! Async.Raise <| new CommunicationTimeoutException("Timeout occurred while trying to send message.", actorId, TimeoutType.MessageWrite)
                with 
                | :? ThespianSerializationException as e ->
                    //this is special; this exception may be thrown
                    //while data to be read still remains on the socket
                    //releasing the connection back to the pool for immediate reuse
                    //would mean the next acquire of the connection would try to write
                    //while there is data to read
                    //so here, we simply close before releasing to the pool
    //              printfn "FAILURE: %A" e
                    protocolStream.Acquire().Dispose()
                    cancelForeignReplyChannels context
                    return! Async.Raise e
                | e ->
    //              printfn "FAILURE: %A" e
                    cancelForeignReplyChannels context
                    protocolStream.Dispose()
                    return! Async.Raise e
            with e ->
                (connection :> IDisposable).Dispose()
                return! Async.Raise e
        }

    let tryPostOnEndpoint endPoint msgId msg withReply =
        async {
            try
                do! attempt (postOnEndpoint endPoint msgId msg withReply)
                return Choice1Of2()
            with 
            | :? SocketException as e when e.SocketErrorCode = SocketError.TimedOut -> return Choice2Of2(new CommunicationTimeoutException("Timeout occurred while trying to establish connection.", actorId, TimeoutType.Connection, e) :> exn)
            | CommunicationException _ as e -> return Choice2Of2 e
            | :? ThespianSerializationException as e -> return Choice2Of2 (e :> exn)
            | e -> return Choice2Of2(new CommunicationException("Communication failure occurred while trying to send message.", actorId, e) :> exn)
        }

    let postMessage msgId msg withReply = async {
        //this is memoized
        let! endPoints = address.ToEndPointsAsync()

        // let! r = tryPostOnEndpoint endPoints.Head msgId msg withReply
        // match r with
        // | Choice1Of2() -> return ()
        // | Choice2Of2 e -> return! Async.Raise e

        let! r = endPoints |> List.foldWhileAsync (fun es endPoint ->
                    async {
                        let! r = tryPostOnEndpoint endPoint msgId msg withReply
                        match r with
                        | Choice1Of2() -> return es, false
                        | Choice2Of2 e ->
        //                    printfn "Trying other endpoint due to: %A" e
        //                    printfn "Endpoints: %A" endPoints
                            return e::es, true
                    }) []

        if r.Length = endPoints.Length then return! Async.Raise r.Head
        else return ()
    }

    let post msg = let msgId = MsgId.NewGuid() in postMessage msgId msg false

    let postMessageWithReply msgId msg timeout =
        async {
            let awaitResponse = replyRegistry.RegisterAndWaitForResponse(msgId, timeout)
            try
                do! postMessage msgId msg true
                let! r = awaitResponse
                match r with
                | Some(Choice1Of2 reply) -> return Some reply
                | Some(Choice2Of2 e) -> return! Async.Raise e
                | None -> return None

            with e ->
                replyRegistry.Unregister(msgId)
                return! Async.Raise e
    }

    member __.TryPostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int) =
        let msgId = MsgId.NewGuid()
        let rc = new ReplyChannel<'R>(actorId, msgId)
        let initTimeout = rc.Timeout
        let msg = msgF (new ReplyChannelProxy<'R>(rc))
        let timeout' = if initTimeout <> rc.Timeout then rc.Timeout else timeout
        async {
            let! response = postMessageWithReply msgId msg timeout'
            match response with
            | Some(Value v) -> return Some (v :?> 'R)
            | Some(Exn e) -> return! Async.Raise (new MessageHandlingException("Remote Actor threw exception while handling message.", actorId, e))
            | None -> return None
        }

    member self.PostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int) =
        async {
            let! r = self.TryPostWithReply(msgF, timeout)
            match r with
            | Some v -> return v
            | None -> return! Async.Raise (new TimeoutException("Timeout occurred while waiting for reply."))
        }

    override __.ToString() = uri

    interface IProtocolClient<'T> with
        override __.ProtocolName = ProtocolName
        override __.ActorId = actorId :> ActorId
        override __.Uri = uri
        override __.Factory = Some (factory :> IProtocolFactory)
        override __.Post(msg: 'T) = Async.RunSynchronously (post msg)
        override __.AsyncPost(msg: 'T) = post msg
        override self.PostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int) = self.PostWithReply(msgF, timeout)
        override self.TryPostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int) = self.TryPostWithReply(msgF, timeout)


and ProtocolServer<'T>(actorName: string, endPoint: IPEndPoint, primary: ActorRef<'T>) =
    let serializer = Serialization.defaultSerializer
    let listener = TcpListenerPool.GetListener(endPoint)
    let listenerAddress = new Address(TcpListenerPool.DefaultHostname, listener.LocalEndPoint.Port)
    let actorId = new TcpActorId(actorName, ProtocolName, listenerAddress)
    let client = new ProtocolClient<'T>(actorId)
  
    let logEvent = new Event<Log>()
    let mutable listenerLogSubcription: IDisposable option = None

    let processMessage ((msgId, actorId, payload, protocolStream): RequestData): Async<bool> = async {
        let protocolMessageStream = new ProtocolMessageStream(protocolStream)
        use context = new DeserializationContext(protocolMessageStream)
        let rw = context.ReplyWriter
        let msg = serializer.Deserialize<obj>(payload, context.GetStreamingContext()) :?> 'T

        //forward message
        primary <-- msg

        try
            let! r = protocolStream.TryAsyncWriteResponse(Acknowledge msgId)
            match r with
            | Some() ->      
                do! rw <!- fun ch -> StartExecs(ch.WithTimeout(Timeout.Infinite))
                //wait until all reply channels have been writen to
                //note: the client is also waiting on the reply channels
                //if the timeout here is the same as the reply channel timeout
                //then the client might see a connection reset before detecting the timeout
                //thus, the timeout here must be bigger
                let! r = protocolMessageStream.AsyncWaitForDisposal(context.MaxReplyChannelTimeout * 2)
                // let! r = protocolMessageStream.AsyncWaitForDisposal(Timeout.Infinite)
                match r with
                | Some _ -> return true
                | None ->
                    //kill the connection, but not concurrently to any pending replies
                    do! rw <!- fun ch -> Exec(ch.WithTimeout(Timeout.Infinite), async { do! Async.Sleep 100 
                                                                                        return protocolStream.Acquire().Dispose() })
                    return false
            | None ->
                do! rw <!- fun ch -> StartExecs(ch.WithTimeout(Timeout.Infinite))
                return false
        with e ->
            //log errror
            logEvent.Trigger(Warning, Protocol "actor", box e)
            protocolStream.Acquire().Dispose()
            return false
    }

    let start() =
        match listenerLogSubcription with
        | None ->
            let observer =
                listener.Log
    //        |> Observable.choose (function Warning, source, (:? CommunicationException as e) when e.ActorId = actorId -> Some(Warning, source, e :> obj) | _ -> None)
                |> Observable.subscribe logEvent.Trigger
                |> Some
            listenerLogSubcription <- observer
            listener.RegisterRecipient(actorId, processMessage)

        | _ -> ()

    let stop() =
        match listenerLogSubcription with
        | Some observer ->
            listener.UnregisterRecipient(actorId)
            observer.Dispose()
            listenerLogSubcription <- None
        | _ -> ()

    member __.Log = logEvent.Publish

    interface IProtocolServer<'T> with
        override __.ProtocolName = ProtocolName
        override __.ActorId = actorId :> ActorId
        override __.Client = client :> IProtocolClient<'T>
        override self.Log = self.Log
        override __.Start() = start()
        override __.Stop() = stop()
        override __.Dispose() = stop()


and ProtocolMode = TcpProtocol.Unidirectional.ProtocolMode
and [<Serializable>] BTcpFactory =
    val private protocolMode: ProtocolMode

    new (protocolMode: ProtocolMode) = { protocolMode = protocolMode }
    internal new (info: SerializationInfo, context: StreamingContext) = { protocolMode = info.GetValue("protocolMode", typeof<ProtocolMode>) :?> ProtocolMode }

    member self.CreateClientInstance(actorName: string) =
        let address = self.protocolMode.GetAddress()
        let actorId = new TcpActorId(actorName, ProtocolName, address)
        new ProtocolClient<'T>(actorId) :> IProtocolClient<'T>

    member self.CreateServerInstance(actorName: string, primary: ActorRef<'T>) =
        match self.protocolMode with
        | ProtocolMode.Server (ip, port) -> 
            let endPoint = IPEndPoint(IPAddress.Parse ip, port)
            new ProtocolServer<'T>(actorName, endPoint, primary) :> IProtocolServer<'T>
        | _ -> invalidOp "Tried to creade utcp protocol server instance using client information."
  
    interface IProtocolFactory with
        override __.ProtocolName = ProtocolName
        override self.CreateServerInstance(actorName: string, primary: ActorRef<'T>): IProtocolServer<'T> = self.CreateServerInstance(actorName, primary)
        override self.CreateClientInstance(actorName: string): IProtocolClient<'T> = self.CreateClientInstance(actorName)

    interface ISerializable with override self.GetObjectData(info: SerializationInfo, context: StreamingContext) = info.AddValue("protocolMode", self.protocolMode)

        
//         [<Serializable>]
//         type BTcp = 
//             inherit TcpProtocolConfiguration 

//             new (publishMode: PublishMode, ?serializerName: string) = {
//                 inherit TcpProtocolConfiguration(publishMode, ?serializerName = serializerName)
//             }

//             internal new (info: SerializationInfo, context: StreamingContext) = {
//                 inherit TcpProtocolConfiguration(info, context)
//             }

//             //server side constructor
//             //auto-select address; the addess selected will be detrmined by TcpListenerPool.Hostname
//             new () = new BTcp(Publish.all)
//             //server side constructor
//             //specific port; the address will be detrmined by TcpListenerPool.Hostname
//             new (port: int, ?serializerName: string) = new BTcp(Publish.endPoints [IPEndPoint.anyIp port], ?serializerName = serializerName)
            
//             override tcp.ProtocolName = ProtocolName
//             override tcp.ConstructProtocols<'T>(actorUUId: ActorUUID, actorName: string, publishMode: PublishMode, actorRef: ActorRef<'T> option, serializerName: string option) =
//                 match publishMode with
//                 | Client addresses ->
//                     addresses |> List.map (fun addr -> new Protocol<'T>(actorUUId, actorName, ClientAddress addr, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T>)
//                               |> List.toArray
//                 | Server [] ->
//                     [| for listener in TcpListenerPool.GetListeners(IPEndPoint.any, ?serializer = serializerName) ->
//                             new Protocol<'T>(actorUUId, actorName, ServerEndPoint listener.LocalEndPoint, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T> |]
//                 | Server ipEndPoints ->
//                     [| for ipEndPoint in ipEndPoints ->
//                             new Protocol<'T>(actorUUId, actorName, ServerEndPoint ipEndPoint, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T> |]

//         //if actorRef is Some then the protocol instance is used in server+client mode
//         //if None it is in client only mode
//         and Protocol<'T> internal (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, actorRef: ActorRef<'T> option, ?serializerName: string) =
//             let serializerName = serializerNameDefaultArg serializerName
//             let serializer = 
//                 if SerializerRegistry.IsRegistered serializerName then 
//                     SerializerRegistry.Resolve serializerName
//                 else invalidArg "Unknown serializer." "serializerName"

//             let clientOnly = actorRef.IsNone

//             //we do not always need a listener
//             //only in server/client mode
//             let listener = 
//                 match actorRef with
//                 | Some _ ->
//                     match addressKind with
//                     | ClientAddress _ -> None
//                     | ServerEndPoint endPoint -> TcpListenerPool.GetListener(endPoint, serializer = serializerName) |> Some
//                 | None -> None

//             let mutable listenerLogSubcription: IDisposable option = None
            
//             let targetAddress =
//                 if clientOnly && ProtocolAddressKind.isServerEndPoint addressKind then invalidArg "protocolAddressKind" "Need to specify address in client only mode."
//                 match addressKind with
//                 | ClientAddress address -> address
//                 | ServerEndPoint endPoint -> new Address(TcpListenerPool.DefaultHostname, endPoint.Port)

//             //let debug i x = Debug.writelfc (sprintf "btcp(%A)::%A" targetAddress i) x

//             let actorId = new TcpActorId(actorUUID, actorName, ProtocolName, targetAddress)

//             let logEvent = new Event<Log>()

//             //response waiting registry
//             //we use an AsyncResultCell to wait for a response
//             //a response is either a Reply, in which case a proper reply has been received,
//             //or not a reply, in which case Some(exn) indicates a communication failure
//             //while None indicates a timeout or cancellation
//             let registeredResponseAsyncWaits = Atom.create Map.empty<MsgId, AsyncResultCell<Choice<Reply<obj>, exn option>>>
//             //unregister a wait for response
//             let unregisterResponseAsyncWait msgId = Atom.swap registeredResponseAsyncWaits (fun regs -> Map.remove msgId regs)
//             //register and get an async that waits for a response
//             let registerAndWaitForResponse msgId timeout =
//                 let resultCell = new AsyncResultCell<_>()

//                 Atom.swap registeredResponseAsyncWaits (fun regs -> Map.add msgId resultCell regs)

//                 async {
//                     let! result = resultCell.AsyncWaitResult(timeout) //throws ResponseAwaitCancelled
//                     unregisterResponseAsyncWait msgId //always unregister in the end
//                     return match result with
//                            | Some r -> r
//                            | None -> Choice2Of2 None
//                 }
//             //cancel waiting for a response, cancelling causes the same result as timeout
//             let cancelResponseWait msgId = 
//                 registeredResponseAsyncWaits.Value.TryFind(msgId) |> Option.iter (fun resultCell -> resultCell.RegisterResult(Choice2Of2 None))
//             //fail waiting for a resposne
//             let failResponseWait e msgId =
//                  registeredResponseAsyncWaits.Value.TryFind(msgId) |> Option.iter (fun resultCell -> resultCell.RegisterResult(Choice2Of2(Some e)))

//             //this receives serialized messages from the listener
//             //deserializes and then passes processProtocolMessage for further processing
//             let recipientProcessorBehavior (actorRef: ActorRef<'T>) ((msgId, payload, protocolStream): recipientProcessor) = 
//                 async {
//                     //debug msgId "MESSAGE PROCESS START"

//                     let protocolMessageStream = new ProtocolMessageStream(protocolStream, serializer)

//                     let deserializationResult = 
//                         try
//                             //second stage deserialization
//                             //if during the deserialization we encounter a reply channel, then the ProtocolMessageStream
//                             //is going to be useNested, preventing its disposal until all reply channels have been replied to
//                             //throws SerializationException
//                             let context = new DeserializationContext(protocolMessageStream)
//                             let msg = serializer.Deserialize<obj>(payload, context.GetStreamingContext()) :?> 'T
//                             Choice1Of2 msg
//                         with e -> Choice2Of2 e

//                     try
//                         match deserializationResult with
//                         | Choice1Of2 msg ->
//                             //sprintfn "DSRL %A" msgId
//                             //send ack back
//                             do! protocolStream.AsyncWriteResponse(Acknowledge msgId)
//                             //if the above fails, we do not want to continue with the message
//                             //because if it is a two-way message the connection will be gone    

//                             //debug msgId "FORWARD TO PRINCIPAL"

//                             //forward message to actor
//                             actorRef <-- msg

//                             //debug msgId "WAITING FOR PROTOCOL MESSAGE STREAM DISPOSAL"
// //                            try
//                             //NOTE!!! This is wrong
//                             //On timeout, we should invalidate all reply channels
//                             //because it might take more than the timeout to respond
//                             let! r = protocolMessageStream.AsyncWaitForDisposal() |> Async.WithTimeout 120000
// //                            do! protocolMessageStream.AsyncWaitForDisposal() |> Async.WithTimeout 120000 |> Async.Ignore
//                             match r with
//                             | Some _ -> ()
//                             | None -> protocolStream.Retain <- false  //on timeout make the listener reset the connection
// //                            with _ ->
// //                                protocolStream.Dispose()
//                             //protocolStream.Retain <- false
//                         | Choice2Of2 e ->
//                             //sprintfn "DSRLFAULT %A %A" msgId e
//                             //probably an exception related to deserialization
//                             //send with fail
//                             do! protocolStream.AsyncWriteResponse(Failure(msgId, e))
//                             //protocolStream.Dispose()
//                             //protocolStream.Retain <- false

//                         //debug msgId "MESSAGE PROCESS END"
//                     with e ->
//                         //debug msgId "ACKNOWLEDGE WRITE FAILURE: %A" e
//                         //writing back protocol resposne failure
//                         logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to write protocol response.", e) |> box)
//                         //protocolStream.Dispose()
//                         //protocolStream.Retain <- false
//                 }

//             let recipientProcessor = 
//                 match actorRef with
//                 | Some actorRef -> Actor.bind <| Behavior.stateless (recipientProcessorBehavior actorRef)
//                 | None -> Actor.sink() //if in client only mode, we are not getting messages from the tcp listener

//             let newSerializationContext() =
//                 new MessageSerializationContext(serializer,
//                     {
//                         new IReplyChannelFactory with 
//                             member f.Protocol = ProtocolName
//                             member f.Create<'R>() = 
//                                 let newMsgId = MsgId.NewGuid()
//                                 new ReplyChannelProxy<'R>(
//                                     new ReplyChannel<'R>(actorId, newMsgId, serializerName)
//                                 )
//                     })

//             let processReply (protocolStream: ProtocolStream) = async {
//                 //debug 0 "WAITING FOR REPLY"
//                 let! replyMsgId, actorId, serializedReply = protocolStream.AsyncReadRequest() //if this fails, then the connection fails

//                 let deserializationResult =
//                     try
//                         //can fail
//                         let reply = serializer.Deserialize<obj>(serializedReply) :?> Reply<obj>
//                         Choice1Of2 reply
//                     with e -> Choice2Of2 e

//                 match deserializationResult, registeredResponseAsyncWaits.Value.TryFind replyMsgId with
//                 | Choice1Of2 reply, Some responseAsyncCell -> //found a waiting reply
//                     //debug replyMsgId "REPLY RECEIVED"
//                     //send back ack
//                     do! protocolStream.AsyncWriteResponse(Acknowledge(replyMsgId)) //if this fails, connection fails

//                     //set reply result
//                     try
//                         responseAsyncCell.RegisterResult(Choice1Of2 reply)
//                         //debug replyMsgId "REPLY REGISTERED"
//                     with e ->
//                         //debug replyMsgId "REPLY RESULT REG FAILURE: %A" e
//                         //setting the result has failed
//                         logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Setting the result for a reply has failed after responding with Acknowledgement.", e) |> box)
//                 | Choice1Of2 _, None -> //no matching waiting reply found
//                     //debug replyMsgId "REPLY RECEIVE UNKNOWN MSG"
//                     //send back unknown recipient
//                     do! protocolStream.AsyncWriteResponse(UnknownRecipient(replyMsgId, actorId)) //if this fails, then the connection fails
//                 | Choice2Of2 e, _ ->
//                     //debug replyMsgId "REPLY RECEIVE DESERIALIZATION FAILURE: %A" e
//                     do! protocolStream.AsyncWriteResponse(Failure(replyMsgId, e)) //if this fails, then the connection fails
//             }

//             let postMessageOnEndPoint (targetEndPoint: IPEndPoint) (msgId: MsgId) (msg: 'T) (timeoutResource: AsyncCountdownLatch option) = async {
//                 //let debug x = debug msgId x
//                 let withReply = timeoutResource.IsSome
//                 //if withReply then debug "POSTING MESSAGE WITH REPY %A" msg else debug "POSTING MESSAGE %A" msg

//                 //debug "CONNECTING"
//                 let! connection = TcpConnectionPool.AsyncAcquireConnection targetEndPoint

//                 let stream = connection.GetStream() //stream.Close(); connection.UnderlyingClient.Close(); 
// //                let protocolStream = new ProtocolStream(msgId, stream, fun () -> sprintfn "CLDISPN %A %A" connection.UnderlyingClient.Client.LocalEndPoint connection.UnderlyingClient.Client.RemoteEndPoint; connection.Return())
//                 let protocolStream = new ProtocolStream(msgId, stream)
//                 use _ = useNested protocolStream

//                 let serializationContext = newSerializationContext()
//                 let serializedMessage = serializer.Serialize<obj>(msg, serializationContext.GetStreamingContext())

//                 //handle foreign reply channels provided in the context
//                 //We will async start one computation waiting for a reply for each reply channel override
//                 //Based on the msgId of the reply on a native rc we find the foreign rc and forward it
//                 let foreignReplyChannelMap = 
//                     serializationContext.ReplyChannelOverrides |> List.map (fun (foreignReplyChannel, nativeReplyChannel) ->
//                         let nativeReplyChannel = nativeReplyChannel :?> ReplyChannel
//                         nativeReplyChannel.MessageId, foreignReplyChannel
//                     ) |> Map.ofList

//                 let timeoutResource = if withReply then timeoutResource.Value else new AsyncCountdownLatch(0)

//                 //handle foreign reply channels provided in the context
//                 let foreignReplyChannelHandlers, foreignReplyChannelMsgIds =
//                     serializationContext.ReplyChannelOverrides |> List.map (fun (foreignReplyChannel, nativeReplyChannel) ->
//                         //the foreign reply channel will not be posted through the message, but the native one will
//                         //we need to wait for the response on the native rc and forward it to the foreign rc
//                         //each native reply channel has a new msgId
//                         let nativeReplyChannel = nativeReplyChannel :?> ReplyChannel
//                         let awaitResponse = registerAndWaitForResponse nativeReplyChannel.MessageId foreignReplyChannel.Timeout
//                         async {
//                             //wait for the native rc result
//                             let! result = awaitResponse
//                             match result with
//                             | Choice1Of2 replyResult ->    
//                                 //forward to foreign rc
//                                 try
//                                     foreignReplyChannel.ReplyUntyped(replyResult)
//                                 with e ->
//                                     logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to forward a reply.", e) |> box)
//                             | Choice2Of2(Some e) ->
//                                 //A communication failure occurred.
//                                 //forward to foreign rc
//                                 try
//                                     foreignReplyChannel.ReplyUntyped(Exception(new CommunicationException("A communication failure occurred while waiting for reply.", e)))
//                                 with e ->
//                                     logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to forward a reply.", e) |> box)
//                             | Choice2Of2 None -> ()
//                                 //Either the wait timed-out or it was cancelled.
//                                 //If the wait-timed out on our native rc, then so it will on the foreign rc.
//                                 //If the response wait was cancelled then we failed to post the message.
//                                 //This means that the client posting the message will be notified by an exception.
//                                 //It is the client's responsibility to handle this and perform any action on the
//                                 //foreign rcs. 
//                                 //Thus, we do nothing in this case.

//                             timeoutResource.Decrement()
                                
//                         },
//                         nativeReplyChannel.MessageId
//                         //we map to pairs of async reply forward computation and the native rc msgId
//                         //because if something goes wrong in the message post
//                         //we need to use the msgIds to unregister the waits
//                     ) |> List.unzip
//                 //start handling the replies for foreign reply channels
//                 foreignReplyChannelHandlers |> Async.Parallel |> Async.Ignore |> Async.Start

//                 //post the message
//                 //create and write protocol request write message
//                 let protocolRequest = msgId, actorId, serializedMessage : ProtocolRequest

//                 try
//                     //debug "POSTING"
//                     do! protocolStream.AsyncWriteRequest(protocolRequest)
//                     //debug "POSTED"
//                     let! response = protocolStream.AsyncReadResponse()
                    
//                     match response with
//                     | Acknowledge _ -> () //debug "POST SUCCESS" //post was successfull
//                     | UnknownRecipient _ -> 
//                         //debug "POST UNKNOWN RECIPIENT"
//                         raise <| new UnknownRecipientException("Remote host could not find the message recipient.")
//                     | Failure(_, e) -> 
//                         //debug "POST FAILURE: %A" e
//                         raise <| new CommunicationException("Message delivery failed on the remote host.", e)

//                     //start reply processing
//                     let expectedReplies = foreignReplyChannelMap.Count + if withReply then 1 else 0
//                     timeoutResource.Set(expectedReplies)
//                     //hold on to the connection until all replies have been dealt with
//                     //the protocol stream has expectedReplies ref count
//                     let protocolStreamResource = useNested protocolStream
//                     async {
//                         use _ = protocolStreamResource
//                         try
//                             let replyProcessing = async {
//                                 for _ in 1..expectedReplies do
//                                     do! processReply protocolStream //this will fail if connection fails
//                             }
//                             let timeoutResourceWait = async {
//                                 do! timeoutResource.AsyncWaitToZero()
//                                 return true
//                             }
//                             //debug "PROCESSING REPLIES"
//                             do! Async.ConditionalCancel timeoutResourceWait replyProcessing |> Async.Ignore
//                             //debug "REPLIES PROCESSED"
//                         with e -> //connection failure
//                             //debug "REPLY PROCESSING FAILURE: %A" e
//                             logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Communication failure while waiting for reply.", e) |> box)
//                             //need to cancel all oustanding reply waits
//                             foreignReplyChannelMsgIds |> List.iter (failResponseWait e)
//                             if withReply then failResponseWait e msgId
//                     } |> Async.Start

//                 with e ->
//                     //debug "POST FAILURE: %A" e
//                     let le, re = if connection.UnderlyingClient.Client <> null then connection.UnderlyingClient.Client.LocalEndPoint, connection.UnderlyingClient.Client.RemoteEndPoint else null, null
//                     //either writing the protocol message, or reading the protocol response failed
//                     //due to connection failure, or due to malformed messaging
//                     //or unboxing fails due to client-server type mismatch
//                     //in any case, we want to cancel waiting for any responses on overriden
//                     //foreign reply channels
//                     //----
//                     //cancel the async handling the foreign rc responses
//                     foreignReplyChannelMsgIds |> List.iter cancelResponseWait
//                     protocolStream.Dispose()

//                     return! raisex e
//             }

//             let postMessageGenericLoop (msgId: MsgId) (msg: 'T) (timeoutResource: AsyncCountdownLatch option) = async {
//                 let! endPoints = targetAddress.ToEndPointsAsync()

//                 return! postMessageOnEndPoint endPoints.[0] msgId msg timeoutResource
//             }

//             let postMessage msgId msg = attempt (postMessageGenericLoop msgId msg None)

//             let postMessageWithReply (msgId: MsgId) (msg: 'T) (timeout: int) = async {
//                 //register and get async wait for response
//                 let awaitResponse = registerAndWaitForResponse msgId timeout
                
//                 let timeoutResource = new AsyncCountdownLatch(0)

//                 //post message
//                 try
//                     do! attempt (postMessageGenericLoop msgId msg (Some timeoutResource))
//                 with CommunicationException(_, _, _, InnerException e) -> 
//                         unregisterResponseAsyncWait msgId
//                         timeoutResource.Decrement()
//                         raise e
//                     | (CommunicationException _) as e ->
//                         unregisterResponseAsyncWait msgId
//                         timeoutResource.Decrement()
//                         raise e
//                     | e -> 
//                         //if smth goes wrong in the post we must unregister the awaitResponse
//                         //we are not cancelling the resposne wait here because we are not waiting
//                         //for it yet
//                         unregisterResponseAsyncWait msgId
//                         timeoutResource.Decrement()
//                         //and reraise the exception
//                         raise <| new CommunicationException("A communication error occurred.", e)
                
//                 //async wait for resposne and return
//                 let! responseResult = awaitResponse
//                 timeoutResource.Decrement()
//                 return match responseResult with
//                        | Choice1Of2 replyResult -> Some replyResult //normal result
//                        | Choice2Of2(Some(CommunicationException(_, _, _, e))) -> raise e //Communication failure
//                        | Choice2Of2(Some e) -> raise <| new CommunicationException("A communication error occurred.", e) //Communication failure
//                        | Choice2Of2 None -> None //timeout
//             }

//             //client+server use constructor
//             new (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, actorRef: ActorRef<'T>, ?serializerName: string) =
//                 new Protocol<'T>(actorUUID, actorName, addressKind, Some actorRef, ?serializerName = serializerName)

//             //client only use constructor
//             new (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, ?serializerName: string) =
//                 new Protocol<'T>(actorUUID, actorName, addressKind, None, ?serializerName = serializerName)

//             //main post message with reply, timeout returns None
//             member private p.TryPostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, ?timeoutOverride: int): Async<'R option> = async {
//                 let msgId = MsgId.NewGuid()

//                 let replyChannel = new ReplyChannel<'R>(actorId, msgId, serializer = serializerName)

//                 let msg = msgBuilder (new ReplyChannelProxy<'R>(replyChannel))
//                 //override timeout set on the reply channel, if timeoutOverride is set
//                 timeoutOverride |> Option.iter (fun timeout -> replyChannel.Timeout <- timeout)

//                 let! result = postMessageWithReply msgId msg replyChannel.Timeout
//                 return result |> Option.map Reply.unbox<'R>
//                               |> Option.map (function Value v -> v | Exception e -> raise (new MessageHandlingException("Remote Actor threw exception while handling message", actorName, actorUUID, e)))
//             }
//             //post with reply, timeout throws exception
//             member private p.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, ?timeoutOverride: int): Async<'R> = async {
//                 let! result = p.TryPostWithReply(msgBuilder, ?timeoutOverride = timeoutOverride)
//                 return match result with
//                         | Some v -> v
//                         | None -> raise <| new TimeoutException("Waiting for the reply has timed-out.")
//             }

//             override p.ToString() =
//                 sprintf "%s://%O.%s" ProtocolName actorId typeof<'T>.Name

//             interface IActorProtocol<'T> with

//                 //TODO!!! Change this so that the configuration always has a serializer name,
//                 //even in the case of the default one
//                 //NOTE!!! When using a default serializer, the serializer name is ""
//                 //We should be able to get the serializer name from the serializer object
//                 member p.Configuration =
//                     new BTcp(ProtocolAddressKind.toPublishMode addressKind, serializerName) :> IProtocolConfiguration |> Some

//                 member p.MessageType = typeof<'T>

//                 member p.ProtocolName = ProtocolName
                
//                 //TODO!!! Check the correctness of these
//                 //this is different behavior and perhaps not the one intended
//                 //need to check the repercutions
//                 member p.ActorUUId = actorUUID
//                 member p.ActorName = actorName
//                 member p.ActorId = actorId :> ActorId

//                 member p.Log = logEvent.Publish

//                 //Post a message
//                 member p.Post(msg: 'T) =
//                     (p :> IActorProtocol<'T>).PostAsync(msg) |> Async.RunSynchronously

//                 member p.PostAsync(msg: 'T): Async<unit> = async {
//                     let msgId = MsgId.NewGuid()
//                     try
//                         do! postMessage msgId msg
//                     with (CommunicationException _) as e -> return raise e
//                          | e -> return raise <| new CommunicationException("A communication error occurred.", e)
//                 }

//                 //TODO!!! Fix IActorProtocols and ActorRefs to have this method with an optional timeout
//                 //like our method outside the iface
//                 member p.TryPostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, timeout: int): Async<'R option> = 
//                     p.TryPostWithReply(msgBuilder, timeout)

//                 //Post a message and wait for reply with infinite timeout
//                 member p.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T): Async<'R> =
//                     p.PostWithReply(msgBuilder)

//                 //Post a message and wait for reply with specified timeout, 
//                 member p.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, timeout: int): Async<'R> =
//                     p.PostWithReply(msgBuilder, timeout)

//                 member p.Start() = 
//                     match listenerLogSubcription with
//                     | None ->
//                         //TODO!!! Fix this
//                         listener |> Option.iter (fun listener ->
//                             listenerLogSubcription <- listener.Log //|> Observable.choose (function Warning, source, (:? SocketResponseException as e) when e.ActorId = actorId -> Some(Warning, source, e :> obj) | _ -> None)
//                                                                    |> Observable.subscribe logEvent.Trigger
//                                                                    |> Some)

//                         recipientProcessor.Start()
//                         listener |> Option.iter (fun listener -> listener.Registerrecipient(actorId, !recipientProcessor, match actorRef with Some actorRef -> recipientProcessorBehavior actorRef | None -> fun _ -> async.Return()))
//                     | _ -> ()

//                 member p.Stop() =
//                     match listenerLogSubcription with
//                     | Some disposable ->
//                         listener |> Option.iter (fun listener -> listener.Unregisterrecipient(actorId))
//                         recipientProcessor.Stop()
//                         disposable.Dispose()
//                         listenerLogSubcription <- None
//                     | None -> ()

