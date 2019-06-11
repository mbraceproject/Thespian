module Nessos.Thespian.Remote.TcpProtocol.Unidirectional

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open System.Collections.Concurrent

open Nessos.Thespian
open Nessos.Thespian.Utils.Async
open Nessos.Thespian.Logging
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Remote.TcpProtocol.ConnectionPool
open Nessos.Thespian.Serialization

let ProtocolName = "utcp"

exception private ReplyResult of Reply<obj> option

type ProtocolMessage<'T> = 
    | Request of 'T
    | Response of Reply<obj>

let rec internal attempt f = 
    async { 
        try return! f
        with
        | :? SocketException as e when e.SocketErrorCode = SocketError.ConnectionReset 
                                       || e.SocketErrorCode = SocketError.ConnectionAborted -> 
            //          printfn "Reattempting..."
            return! attempt f
        | :? EndOfStreamException -> 
            //          printfn "Reattempting..."
            return! attempt f
        | :? IOException as e -> 
            match e.InnerException with
            | :? SocketException as e' when e'.SocketErrorCode = SocketError.ConnectionReset 
                                            || e'.SocketErrorCode = SocketError.ConnectionAborted -> 
                //                                   printfn "Reattempting..."                                   
                return! attempt f
            | _ -> return! Async.Raise e
    }

let inline internal addressToEndpoints (actorId : ActorId) (address : Address) = 
    async { 
        try return! address.ToEndPointsAsync()
        with e -> return! Async.Raise <| new CommunicationException("Unable to obtain ip endpoint from address.", actorId, e)
    }

module ProtocolMessage = 
    let box (protocolMessage : ProtocolMessage<'T>) : ProtocolMessage<obj> = 
        match protocolMessage with
        | Request r -> Request(box r)
        | Response r -> Response r
    
    let unbox<'T> (protocolMessage : ProtocolMessage<obj>) : ProtocolMessage<'T> = 
        match protocolMessage with
        | Request r -> Request(unbox r)
        | Response r -> Response r

type ProtocolMessageStream(msgId : MsgId, stream : ProtocolNetworkStream, timeout : int, ?tracePrefix : string) = 
    let tracePrefix = defaultArg tracePrefix String.Empty
    let protocolStream = new ProtocolStream(msgId, stream, timeout, timeout)
    new(msgId : MsgId, client : PooledTcpClient, timeout : int, ?tracePrefix : string) = 
        new ProtocolMessageStream(msgId, client.GetStream(), timeout, ?tracePrefix = tracePrefix)

    member __.ProtocolStream = protocolStream
    
    member __.TryAsyncWriteProtocolMessage(actorId : TcpActorId, protocolMessage : ProtocolMessage<'T>, 
                                           ?serializationContext : MessageSerializationContext) : Async<unit option> = 
        async { 
            let ctx = serializationContext |> Option.map (fun mc -> mc.GetStreamingContext())
            let serializedProtocolMessage = 
                defaultSerializer.Serialize<ProtocolMessage<'T>>(protocolMessage, ?context = ctx)
            let protocolRequest : ProtocolRequest = msgId, actorId, serializedProtocolMessage
            return! protocolStream.TryAsyncWriteRequest(protocolRequest)
        }
    
    member __.Dispose() = protocolStream.Dispose()
    override __.GetHashCode() = protocolStream.GetHashCode()
    
    override __.Equals(other : obj) = 
        match other with
        | :? ProtocolMessageStream as otherStream -> protocolStream.Equals(otherStream.ProtocolStream)
        | _ -> false
    
    interface IDisposable with
        member self.Dispose() = self.Dispose()


//currently used as base for btcp deserialization contexts
type DeserializationContext() = 
    let mutable maxReplyChannelTimeout = 0
    member __.MaxReplyChannelTimeout = maxReplyChannelTimeout
    
    member __.TrySetMaxReplyChannelTimeout(timeout : int) = 
        if timeout > maxReplyChannelTimeout then maxReplyChannelTimeout <- timeout
    
    member self.GetStreamingContext() = new StreamingContext(StreamingContextStates.All, self)
                                    

[<AbstractClass; Serializable>]
type ReplyChannel = 
    val mutable private timeout : int
    val private msgId : MsgId
    val private actorId : TcpActorId
    
    new(actorId : TcpActorId, msgId : MsgId) = 
        { timeout = Default.ReplyReceiveTimeout
          actorId = actorId
          msgId = msgId }
    
    internal new(info : SerializationInfo, context : StreamingContext) = 
        let timeout = info.GetInt32("timeout")
        match context.Context with
        | :? DeserializationContext as deserializationContext -> 
            deserializationContext.TrySetMaxReplyChannelTimeout(timeout)
        | _ -> invalidArg "context" "Invalid deserialization context given."
        { timeout = timeout
          actorId = info.GetValue("actorId", typeof<TcpActorId>) :?> TcpActorId
          msgId = info.GetValue("msgId", typeof<MsgId>) :?> MsgId }
    
    member r.ActorId = r.actorId
    member r.MessageId = r.msgId
    
    member r.Timeout 
        with get () = r.timeout
        and set (timeout' : int) = r.timeout <- timeout'
    
    member r.GetObjectData(info : SerializationInfo, context : StreamingContext) = 
        info.AddValue("timeout", r.timeout)
        info.AddValue("actorId", r.actorId)
        info.AddValue("msgId", r.msgId)
    
    interface ISerializable with
        member self.GetObjectData(info : SerializationInfo, context : StreamingContext) = 
            self.GetObjectData(info, context)

[<Serializable>]
type ReplyChannel<'T> =
    inherit ReplyChannel
        
    val private replyAddress: Address

    new (replyAddress: Address, actorId: TcpActorId, msgId: MsgId) = {
        inherit ReplyChannel(actorId, msgId)            
        replyAddress = replyAddress
    }

    internal new (info: SerializationInfo, context: StreamingContext) = {
        inherit ReplyChannel(info, context)
        replyAddress = info.GetValue("replyAddress", typeof<Address>) :?> Address
    }

    member self.ReplyAddress = self.replyAddress

    member self.AsyncReplyOnEndPoint(endPoint: IPEndPoint, reply: Reply<'T>) =
        async {
            use! connection = TcpConnectionPool.AsyncAcquireConnection(self.MessageId.ToString(), endPoint)
            use protocolMessageStream = new ProtocolMessageStream(self.MessageId, connection, self.Timeout)
            let! r = protocolMessageStream.TryAsyncWriteProtocolMessage<obj>(self.ActorId, Response(Reply.box reply))
            match r with
            | Some() -> return! protocolMessageStream.ProtocolStream.TryAsyncReadResponse()
            | None -> 
                let msg = "Timeout occurred while trying to write reply on channel."
                return! Async.Raise (new CommunicationTimeoutException(msg, self.ActorId, TimeoutType.MessageWrite))
    }

    member self.TryAsyncReplyOnEndPoint(endPoint: IPEndPoint, reply: Reply<'T>) =
        async {
            try
                let! protocolResponse = attempt <| self.AsyncReplyOnEndPoint(endPoint, reply)

                return 
                    match protocolResponse with
                    | Some(Acknowledge _) -> Choice1Of2()
                    | Some(UnknownRecipient _) -> Choice2Of2(new UnknownRecipientException("Reply recipient not found on the other end of the reply channel.", self.ActorId) :> exn)
                    | Some(Failure(_, e)) -> Choice2Of2(new DeliveryException("Failure occurred on the other end of the reply channel.", self.ActorId, e) :> exn)
                    | None -> Choice2Of2(new CommunicationTimeoutException("Timeout occurred while waiting for confirmation of reply delivery.", self.ActorId, TimeoutType.ConfirmationRead) :> exn)

            with 
            | :? SocketException as e when e.SocketErrorCode = SocketError.TimedOut -> return Choice2Of2(new CommunicationTimeoutException("Timeout occurred while trying to establish connection.", self.ActorId, TimeoutType.Connection, e) :> exn)
            | CommunicationException _ as e -> return Choice2Of2 e
            | e -> return Choice2Of2(new CommunicationException("Communication failure occurred while trying to send reply.", self.ActorId, e) :> exn)
        }

    member self.AsyncReply(reply: Reply<'T>) =
        async {
            let! endPoints = addressToEndpoints self.ActorId self.replyAddress

            let! r = 
                endPoints 
                |> List.foldWhileAsync (fun es endPoint ->
                    async {
                        let! r = self.TryAsyncReplyOnEndPoint(endPoint, reply)
                        match r with
                        | Choice1Of2() -> return es, false
                        | Choice2Of2 e -> return e::es, true
                    }) []

            if r.Length = endPoints.Length then return! Async.Raise r.Head
            else return ()
        }

    member self.AsyncReplyUntyped(reply: Reply<obj>) = self.AsyncReply(Reply.unbox reply)

    member self.Reply(reply: Reply<'T>) = Async.RunSynchronously <| self.AsyncReply(reply)
    member self.ReplyUntyped(reply: Reply<obj>) = self.Reply(Reply.unbox reply)

    interface IReplyChannel<'T> with
        override self.Protocol = ProtocolName
        override self.Timeout with get() = self.Timeout and set(timeout': int) = self.Timeout <- timeout'    
        override self.AsyncReplyUntyped(reply: Reply<obj>): Async<unit> = Async.Isolate <| self.AsyncReplyUntyped(reply)
        override self.AsyncReply(reply: Reply<'T>): Async<unit> = Async.Isolate <| self.AsyncReply(reply)

    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            info.AddValue("replyAddress", self.replyAddress)
            base.GetObjectData(info, context)


type MessageProcessor<'T> private (actorId : TcpActorId, listener : TcpProtocolListener, processRequest : 'T -> unit) = 
    let serializer = Serialization.defaultSerializer
    let logEvent = new Event<Log>()
    let clientRegistry = new ConcurrentDictionary<MsgId, Reply<obj> -> unit>()
    
    [<VolatileField>]
    let mutable refCount = 0
    
    [<VolatileField>]
    let mutable isReleased = false
    
    let spinLock = SpinLock(false)
    
    //this receives serialized messages from the listener
    //deserializes and then passes processProtocolMessage for further processing
    //protected in TcpListener
    let processMessage ((msgId, actorId, payload, protocolStream) : RequestData) : Async<bool> = 
        async { 
            //second stage deserialization
            //throws SerializationException, InvalidCastException
            let ctx = new DeserializationContext()
            let msg = 
                serializer.Deserialize<ProtocolMessage<obj>>(payload, ctx.GetStreamingContext()) 
                |> ProtocolMessage.unbox<'T>
            match msg with
            | Request request -> 
                try 
                    do processRequest request
                    let! r = protocolStream.TryAsyncWriteResponse(Acknowledge msgId)
                    return Option.isSome r
                with e -> 
                    //TODO!!! Fix this exception
                    logEvent.Trigger
                        (Warning, LogSource.Protocol ProtocolName, 
                         new CommunicationException("Failed to respond with Acknowledgement.", actorId, e) |> box)
                    return false
            | Response reply -> 
                let isValid, k = clientRegistry.TryRemove msgId
                if isValid then 
                    k reply
                    try 
                        let! r = protocolStream.TryAsyncWriteResponse(Acknowledge msgId)
                        return Option.isSome r
                    with e -> 
                        logEvent.Trigger
                            (Warning, LogSource.Protocol ProtocolName, 
                             new CommunicationException("Failed to respond with Acknowledgement.", actorId, e) |> box)
                        return false
                else 
                    try 
                        let! r = protocolStream.TryAsyncWriteResponse(UnknownRecipient(msgId, actorId))
                        return Option.isSome r
                    with e -> 
                        logEvent.Trigger
                            (Warning, LogSource.Protocol ProtocolName, 
                             
                             new CommunicationException("Failed to send an UknownRecipient response.", actorId, e) 
                             |> box)
                        return false
        }
    
    static let processors = new ConcurrentDictionary<TcpActorId, MessageProcessor<'T>>()
    member __.RegisterReplyProcessor(msgId : MsgId, replyF : Reply<obj> -> unit) = clientRegistry.TryAdd(msgId, replyF) |> ignore
    member __.UnregisterReplyProcessor(msgId : MsgId) = clientRegistry.TryRemove(msgId) |> ignore
    
    member __.Acquire() = 
        let taken = ref false
        spinLock.Enter(taken)
        if isReleased then 
            spinLock.Exit()
            false
        else 
            if refCount = 0 then listener.RegisterRecipient(actorId, processMessage)
            refCount <- refCount + 1
            spinLock.Exit()
            true
    
    member self.AcquireAndAllocate() = 
        if processors.TryAdd(actorId, self) then 
            isReleased <- false
            self.Acquire() |> ignore
        else invalidOp "Tried to acquire more than one server processor for the same actor."
    
    member __.Release() = 
        let taken = ref false
        spinLock.Enter(taken)
        if refCount = 1 then 
            isReleased <- true
            listener.UnregisterRecipient(actorId)
            processors.TryRemove(actorId) |> ignore
        refCount <- refCount - 1
        spinLock.Exit()
    
    static member GetClientProcessor(actorId : TcpActorId, listener : TcpProtocolListener) = 
        let p = processors.GetOrAdd(actorId, fun _ -> new MessageProcessor<'T>(actorId, listener, fun _ -> invalidOp "Tried to process a Request message in a protocol client"))
        if p.Acquire() then p
        else 
            Thread.SpinWait(20)
            MessageProcessor<'T>.GetClientProcessor(actorId, listener)
    
    static member GetServerProcessor(actorId : TcpActorId, listener : TcpProtocolListener, processRequest : 'T -> unit) = 
        new MessageProcessor<'T>(actorId, listener, processRequest)

    interface IDisposable with member self.Dispose() = self.Release()
    

type ReplyResultRegistry<'T>(actorId : TcpActorId, listener : TcpProtocolListener) = 
    let results = new ConcurrentDictionary<MsgId, TaskCompletionSource<Reply<obj>>>()
    
    member self.RegisterAndWaitForResponse(msgId : MsgId, timeout : int) = 
        let tcs = new TaskCompletionSource<_>()
        results.TryAdd(msgId, tcs) |> ignore
        let p = MessageProcessor<'T>.GetClientProcessor(actorId, listener)
        p.RegisterReplyProcessor(msgId, fun r -> self.SetResult(msgId, r))
        async { 
            try 
                try 
                    return! Async.AwaitTask(tcs.Task, timeout)
                with _ -> return None
            finally
                p.UnregisterReplyProcessor(msgId)
                p.Release()
        }
    
    member __.Unregister(msgId : MsgId) = 
        let isValid, tcs = results.TryRemove(msgId)
        if isValid then tcs.SetCanceled()
    
    member __.SetResult(msgId : MsgId, value : Reply<obj>) = 
        let isValid, tcs = results.TryRemove(msgId)
        if isValid then tcs.SetResult(value)
    

type ProtocolClient<'T>(actorId: TcpActorId) =
    let address = actorId.Address
    let serializer = Serialization.defaultSerializer
    let localListener = TcpListenerPool.GetListener()
    let localListenerAddress = new Address(TcpListenerPool.DefaultHostname, localListener.LocalEndPoint.Port)
    let serializationContext() =
        new MessageSerializationContext(serializer,
            {
                new IReplyChannelFactory with
                    override __.Protocol = ProtocolName
                    override __.IsForeignChannel<'U>(rc: IReplyChannel<'U>) = rc.Protocol <> ProtocolName
                    override __.Create<'R>() = 
                        let newMsgId = MsgId.NewGuid()
                        new ReplyChannelProxy<'R>(new ReplyChannel<'R>(localListenerAddress, actorId, newMsgId))
            })

    let replyRegistry = new ReplyResultRegistry<'T>(actorId, localListener)
    let logEvent = new Event<Log>()
    let factory = new UTcpFactory(Client address)
    let uri = let ub = new UriBuilder(ProtocolName, address.HostnameOrAddress, address.Port, actorId.Name) in ub.Uri.ToString()

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
            try do! foreignRc.AsyncReplyUntyped reply
            with e -> logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to forward a reply.", e) |> box)
            | None -> ()
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

    let postOnEndpoint targetEndPoint msgId msg =
        async {
            use! connection = TcpConnectionPool.AsyncAcquireConnection(msgId.ToString(), targetEndPoint)
            //using default timeouts here
            use protocolStream = new ProtocolStream(msgId, connection.GetStream())
      
            //serialize message with the reply patching serialization context
            let context = serializationContext()
            let protocolMessage = serializer.Serialize(Request msg |> ProtocolMessage.box, context.GetStreamingContext())

            let foreignRcHandle = setupForeignReplyChannelHandler context

            let protocolRequest = msgId, actorId, protocolMessage : ProtocolRequest
            try
                let! r = protocolStream.TryAsyncWriteRequest(protocolRequest)
                match r with
                | Some() ->
                    Async.Start foreignRcHandle
                    try return! protocolStream.TryAsyncReadResponse()
                    with :? ThespianSerializationException as e -> return! Async.Raise (new CommunicationException("Unable to verify message delivery.", actorId, e))
                | None -> return! Async.Raise (new CommunicationTimeoutException("Timeout occurred while trying to send message.", actorId, TimeoutType.MessageWrite))
            with e ->
                cancelForeignReplyChannels context
                return! Async.Raise e
        }

    let tryPostOnEndpoint endPoint msgId msg =
        async {
            try
                let! protocolResponse = attempt (postOnEndpoint endPoint msgId msg)
                return 
                    match protocolResponse with
                    | Some(Acknowledge _) -> Choice1Of2() //post was successfull
                    | Some(UnknownRecipient _) -> Choice2Of2(new UnknownRecipientException("Remote host could not find the message recipient.", actorId) :> exn)
                    | Some(Failure(_, e)) ->  Choice2Of2(new DeliveryException("Message delivery failed on the remote host.", actorId, e) :> exn)
                    | None -> Choice2Of2(new CommunicationTimeoutException("Timeout occurred while waiting for confirmation of message delivery.", actorId, TimeoutType.ConfirmationRead) :> exn)

            with 
            | :? SocketException as e when e.SocketErrorCode = SocketError.TimedOut -> return Choice2Of2(new CommunicationTimeoutException("Timeout occurred while trying to establish connection.", actorId, TimeoutType.Connection, e) :> exn)
            | CommunicationException _ as e -> return Choice2Of2 e
            | :? ThespianSerializationException as e -> return Choice2Of2 (e :> exn)
            | e -> return Choice2Of2(new CommunicationException("Communication failure occurred while trying to send message.", actorId, e) :> exn)
        }

    let postMessage msgId msg =
        async {
            //this is memoized
            let! endPoints = addressToEndpoints actorId address

            let! r = 
                endPoints |> List.foldWhileAsync (fun es endPoint -> async {
                    let! r = tryPostOnEndpoint endPoint msgId msg
                    match r with
                    | Choice1Of2() -> return es, false
                    | Choice2Of2 e -> return e::es, true 
                    }) []

          if r.Length = endPoints.Length then return! Async.Raise r.Head
          else return ()
        }

    let post msg = let msgId = MsgId.NewGuid() in postMessage msgId msg

    let postMessageWithReply msgId msg timeout = async {
        let awaitResponse = replyRegistry.RegisterAndWaitForResponse(msgId, timeout)
        try
            do! postMessage msgId msg
            return! awaitResponse
        with e ->
            replyRegistry.Unregister(msgId)
            return! Async.Raise e
    }

    member  __.TryPostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int) =
        let msgId = MsgId.NewGuid()
        let rc = new ReplyChannel<'R>(localListenerAddress, actorId, msgId)
        let initTimeout = rc.Timeout
        let msg = msgF (new ReplyChannelProxy<'R>(rc))
        let timeout' = if initTimeout <> rc.Timeout then rc.Timeout else timeout
        async {
            let! resposne = postMessageWithReply msgId msg timeout'
            match resposne with
            | Some (Value v) -> return Some (v :?> 'R)
            | Some (Exn e) -> return! Async.Raise (new MessageHandlingException("Remote Actor threw exception while handling message.", actorId, e))
            | None -> return None
        }

    member self.PostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int) = async {
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
  
    let logEvent = new Event<Log>()
    let mutable listenerLogSubcription: IDisposable option = None

    let processMessage msg = primary <-- msg
    let messageProcessor = MessageProcessor<'T>.GetServerProcessor(actorId, listener, processMessage)

    let client = new ProtocolClient<'T>(actorId)

    let start() =
        match listenerLogSubcription with
        | None ->
            let observer =
                listener.Log
        //        |> Observable.choose (function Warning, source, (:? CommunicationException as e) when e.ActorId = actorId -> Some(Warning, source, e :> obj) | _ -> None)
                |> Observable.subscribe logEvent.Trigger
                |> Some
            listenerLogSubcription <- observer
            messageProcessor.AcquireAndAllocate()

        | _ -> ()

    let stop() =
        match listenerLogSubcription with
        | Some observer ->
            messageProcessor.Release()
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

and ProtocolMode =
    | Client of Address
    | Server of ip:string * port:int
with
    //results are valid only when collocated with the actor
    member self.GetAddress() =
        match self with
        | Client address -> address
        | Server (_,port) -> new Address(TcpListenerPool.DefaultHostname, port)

    static member FromIpEndpoint(ip : IPEndPoint) =
        Server(ip.Address.ToString(), ip.Port)


and [<Serializable>] UTcpFactory =
    val private protocolMode: ProtocolMode

    new (protocolMode: ProtocolMode) = { protocolMode = protocolMode }
    
    internal new (info: SerializationInfo, context: StreamingContext) = { protocolMode = info.GetValue("protocolMode", typeof<ProtocolMode>) :?> ProtocolMode }

    member self.CreateClientInstance(actorName: string) =
        let address = self.protocolMode.GetAddress()
        let actorId = new TcpActorId(actorName, ProtocolName, address)
        new ProtocolClient<'T>(actorId) :> IProtocolClient<'T>

    member self.CreateServerInstance(actorName: string, primary: ActorRef<'T>) =
        match self.protocolMode with
        | Server (ip, port) -> 
            let ipAddr = IPEndPoint(IPAddress.Parse ip, port)
            new ProtocolServer<'T>(actorName, ipAddr, primary) :> IProtocolServer<'T>
        | _ -> invalidOp "Tried to creade utcp protocol server instance using client information."
  
    interface IProtocolFactory with
        override __.ProtocolName = ProtocolName
        override self.CreateServerInstance(actorName: string, primary: ActorRef<'T>): IProtocolServer<'T> = self.CreateServerInstance(actorName, primary)
        override self.CreateClientInstance(actorName: string): IProtocolClient<'T> = self.CreateClientInstance(actorName)

    interface ISerializable with 
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) = info.AddValue("protocolMode", self.protocolMode)

//TODO move this to other file
// let (|Port|_|) (port: string) =
//   match Int32.TryParse(port) with
//   | (true, portNum) -> Some portNum
//   | _ -> None

// let tryFromUri (uri: string) =
//   match uri with
//   | RegEx.Match @"^(.+)://(.+):(\d+)/(.+)$" (_ :: protocol::hostnameOrAddress::Port(port)::name::[]) when protocol = ProtocolName ->
//     let address = new Address(hostnameOrAddress, port)
//     let factory = new UTcpFactory(Client address)
//     let client = factory.CreateClientInstance(name)
//     Some client
//   | _ -> None


// [<Serializable>]
// type UTcp =
//   inherit TcpProtocolConfiguration 

//   new (publishMode: PublishMode, ?serializerName: string) = {
//     inherit TcpProtocolConfiguration(publishMode, ?serializerName = serializerName)
//   }
            
//             //server side constructor
//             //auto-select address; the addess selected will be detrmined by TcpListenerPool.Hostname
//             new () = new UTcp(Publish.all)
//             //server side constructor
//             //specific port; the address will be detrmined by TcpListenerPool.Hostname
//             new (port: int, ?serializerName: string) = 
//                 new UTcp(Publish.endPoints [IPEndPoint.anyIp port], ?serializerName = serializerName)

//             internal new (info: SerializationInfo, context: StreamingContext) = {
//                 inherit TcpProtocolConfiguration(info, context)
//             }

//             override tcp.ProtocolName = ProtocolName
//             override tcp.ConstructProtocols<'T>(actorUUId: ActorUUID, actorName: string, publishMode: PublishMode, actorRef: ActorRef<'T> option, serializerName: string option) =
//                 match publishMode with
//                 | Client addresses ->
//                     if List.isEmpty addresses then raise <| new TcpProtocolConfigurationException("No addresses specified for utcp protocol.")
//                     addresses |> List.map (fun addr ->
//                         match ServerSideProtocolPool.TryGet(new TcpActorId(actorUUId, actorName, ProtocolName, addr)) with
//                         | Some protocol when protocol.MessageType = typeof<'T> -> protocol :?> IActorProtocol<'T>
//                         | _ -> new Protocol<'T>(actorUUId, actorName, ClientAddress addr, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T>
//                     ) |> List.toArray
//                 | Server [] ->
//                     let listeners = TcpListenerPool.GetListeners(IPEndPoint.any, ?serializer = serializerName)
//                     if Seq.isEmpty listeners then raise <| new TcpProtocolConfigurationException("No available listeners found.")
//                     [| for listener in listeners ->
//                             new Protocol<'T>(actorUUId, actorName, ServerEndPoint listener.LocalEndPoint, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T> |]
//                 | Server ipEndPoints ->
//                     [| for ipEndPoint in ipEndPoints ->
//                             new Protocol<'T>(actorUUId, actorName, ServerEndPoint ipEndPoint, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T> |]


//         and internal ServerSideProtocolPool() =
//             static let pool = Atom.create Map.empty<TcpActorId, IActorProtocol>

//             static member Register(actorId: TcpActorId, protocol: IActorProtocol) =
//                 Atom.swap pool (Map.add actorId protocol)

//             static member UnRegister(actorId: TcpActorId) = Atom.swap pool (Map.remove actorId)

//             static member TryGet(actorId: TcpActorId): IActorProtocol option = pool.Value.TryFind actorId
        
//         and internal ListenerRegistrationResource(clientOnly: bool, actorId: ActorId, listener: TcpProtocolListener, recipientProcessor: Actor<recipientProcessor>, processorF: recipientProcessor -> Async<unit>, onDisposeF: unit -> unit) =
//             static let counter = Nessos.Thespian.Agents.Agent.start Map.empty<ActorId, int>

//             let start () =
//                 if clientOnly then
//                     recipientProcessor.Start()
//                     listener.Registerrecipient(actorId, !recipientProcessor, processorF)

//             let stop () =
//                 if clientOnly then
//                     listener.Unregisterrecipient(actorId)
//                     recipientProcessor.Stop()

//             let inc counterMap = 
//                 match Map.tryFind actorId counterMap with 
//                 | Some count -> 
//                     Map.add actorId (count + 1) counterMap
//                 | None -> start(); Map.add actorId 1 counterMap

//             let dec counterMap =
//                 match Map.tryFind actorId counterMap with
//                 | Some 1 -> stop(); Map.remove actorId counterMap
//                 | Some count -> Map.add actorId (count - 1) counterMap
//                 | None -> counterMap

//             do if clientOnly then Nessos.Thespian.Agents.Agent.sendSafe inc counter

//             interface IDisposable with
//                 member d.Dispose() = 
//                     onDisposeF()
//                     if clientOnly then Nessos.Thespian.Agents.Agent.sendSafe dec counter                    

//         //if actorRef is Some then the protocol instance is used in server+client mode
//         //if None it is in client only mode
//         and Protocol<'T> internal (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, actorRef: ActorRef<'T> option, ?serializerName: string) =
//             let serializerName = serializerNameDefaultArg serializerName
//             let serializer = 
//                 if SerializerRegistry.IsRegistered serializerName then 
//                     SerializerRegistry.Resolve serializerName
//                 else invalidArg "Unknown serializer." "serializerName"


//             let clientOnly = Option.isNone actorRef

//             //we always need a listener
//             //in client mode, this is allocated automatically
//             //in server/client mode, this may be allocated automatically or via specified address
//             let listener =
//                 match addressKind with
//                 | ServerEndPoint endPoint -> TcpListenerPool.GetListener(endPoint, serializer = serializerName)
//                 | ClientAddress address -> TcpListenerPool.GetListener(serializer = serializerName)

//             let listenerAddress = new Address(TcpListenerPool.DefaultHostname, listener.LocalEndPoint.Port)

//             //let debug i x = Debug.writelfc (sprintf "utcp(%A)::%A" listenerAddress i) x
            
//             let mutable listenerLogSubcription: IDisposable option = None

//             let address = 
//                 match addressKind with
//                 | ClientAddress targetAddress -> targetAddress
//                 | ServerEndPoint endPoint -> new Address(TcpListenerPool.DefaultHostname, listener.LocalEndPoint.Port)

//             let actorId = TcpActorId(actorUUID, actorName, ProtocolName, address)

//             let logEvent = new Event<Log>()

//             let protocolPing targetEndPoint = 
//                 let debug x = Debug.writelfc (sprintf "utcp::ProtocolPing-%A" targetEndPoint) x
//                 let ping = async {
//                     debug "CONENCTING"
//                     use! connection = TcpConnectionPool.AsyncAcquireConnection("ProtocolPing", targetEndPoint)
                    
//                     use protocolStream = new ProtocolStream(MsgId.Empty, connection.GetStream())
                    
//                     do! protocolStream.AsyncWriteRequest(MsgId.Empty, actorId, Array.empty)
//                     debug "PINGED"

//                     let! response = protocolStream.AsyncReadResponse()
//                     match response with
//                     | Acknowledge msgId when msgId = MsgId.Empty -> debug "PING ACKNOWLEDGED"
//                     | _ -> ()
//                 }
//                 async {
//                     try
//                         return! attempt ping
//                     with e ->
//                         //debug "PING FAILURE: %A" e
//                         return raise (new CommunicationException("A communication error occurred.", e))
//                 }


//             //This is a map for response message handling
//             //There may be many client instances for the same actor
//             //When posting with reply each registers a handler here per msgid
//             //The handler is deregistered on response process
//             static let responseHandleRegistry = Atom.create Map.empty<MsgId, ProtocolStream -> MsgId -> obj -> Async<unit>>

//             //response waiting registry
//             let registeredResponseAsyncWaits = Atom.create Map.empty<MsgId, AsyncResultCell<Reply<obj> option>>
//             //unregister a wait for response
//             let unregisterResponseAsyncWait msgId = Atom.swap registeredResponseAsyncWaits (fun regs -> Map.remove msgId regs)
//             //cancel waiting for a response, cancelling causes the same result as timeout
//             let cancelResponseWait msgId = registeredResponseAsyncWaits.Value.[msgId].RegisterResult None
//             //register and get an async that waits for a response
//             let registerAndWaitForResponse pingAsync msgId timeout = 
//                 let resultCell = new AsyncResultCell<Reply<obj> option>()

//                 Atom.swap registeredResponseAsyncWaits (fun regs -> Map.add msgId resultCell regs)
//                 let rec waitForReply attempts = async { //NOTE!!! Try finally?
//                         let! result = resultCell.AsyncWaitResult(timeout) //throws ResponseAwaitCancelled
//                         //debug (sprintf "MESSAGE POST::%A" msgId) "REPLY RECEIVED"
//                         unregisterResponseAsyncWait msgId //always unregister in the end
//                         match result with
//                         | Some reply -> return reply //if this is None then the wait was cancelled
//                         | None -> //timeout
//                             if attempts = 0 then
//                                 return None
//                             else
//                                 do! pingAsync
//                                 return! waitForReply (attempts - 1)
                                            
//                     }
//                 waitForReply 3
//                 //op

//             let serializationContext address =
//                 new MessageSerializationContext(serializer,
//                     {
//                         new IReplyChannelFactory with 
//                             member f.Protocol = ProtocolName
//                             member f.Create<'R>() = 
//                                 let newMsgId = MsgId.NewGuid()
//                                 new ReplyChannelProxy<'R>(
//                                     new ReplyChannel<'R>(address, actorId, newMsgId, serializerName)
//                                 )
//                     })

//             let handleResponseMsg (protocolStream: ProtocolStream) msgId (responseMsg: obj) = 
//                 //let debug x = debug msgId x
//                 async {
//                     match responseMsg with
//                     | :? Reply<obj> as untypedReply ->
//                         //find result cell and register result
//                         match registeredResponseAsyncWaits.Value.TryFind msgId with
//                         | Some asyncResultCell ->
//                             //debug "REPLY RECEIVED"
//                             //send acknowledgement to client and close the connection
//                             try 
//                                 do! protocolStream.AsyncWriteResponse(Acknowledge msgId)
//                             with e -> 
//                                 //debug "REPLY ACK WRITE FAILURE: %A" e
//                                 //writing the ack has failed
//                                 logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to respond with Acknowledgement.") |> box)

//                             try 
//                                 asyncResultCell.RegisterResult(Some untypedReply)
//                                 //debug "REPLY REGISTERED"
//                             with e ->
//                                 //debug "REPLY RESULT REG FAILURE: %A" e
//                                 //setting the result has failed
//                                 logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Setting the result for a reply has failed after responding with Acknowledgement.", e) |> box)
//                         | None ->
//                             //debug "REPLY RECEIVE UNKNOWN MSG"
//                             logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Received a response for a non-registered message.") |> box)
//                             try
//                                 do! protocolStream.AsyncWriteResponse(Failure(msgId, new System.Collections.Generic.KeyNotFoundException("Response message is of unknown message id.")))
//                             with e ->
//                                 //debug "REPLY FAIL WRITE FAILURE: %A" e
//                                 //writing the failure has failed
//                                 logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to respond with Fail.") |> box)
//                     | _ ->
//                         //the responseMsg is of the wrong type, malformed message
//                         //send failure and close connection
//                         try
//                             //debug "REPLY INVALID"
//                             logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Received a response message of wrong type.") |> box)
//                             do! protocolStream.AsyncWriteResponse(Failure(msgId, new InvalidCastException("Response message payload is of wrong type.")))
//                         with e ->
//                             //debug "REPLY FAIL WRITE FAILURE: %A" e
//                             //writing the failure has failed
//                             logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to respond with Fail.") |> box)

//                         //NOTE!!! We do not register a result to the asyncResultCell
//                         //The failure has been logged, and the sender has been notified
//                         //The sender can then handle this by sending correct data,
//                         //or the reply channel times-out (if timeout is set)
            
//                 }
//             //this processes the UTcp protocol messages
//             //processing varies wether the protocol is used in server+client mode
//             //or client only mode.
//             let processProtocolMessage: MsgId -> ProtocolMessage<'T> -> ProtocolStream -> Async<unit> =
//                 match actorRef with
//                 | Some actorRef -> //protocol is configured for client/server use
//                     fun msgId msg protocolStream -> async {
//                         match msg with
//                         | Request requestMsg ->
//                             try
//                                 //debug msgId "FORWARD TO PRINCIPAL"
//                                 //forward the request message to the actor
//                                 actorRef <-- requestMsg

//                                 //send acknowledgement to client and close the connection
//                                 try
//                                     do! protocolStream.AsyncWriteResponse(Acknowledge msgId) //this may fail if the connection fails
//                                 with e -> //writing the ack has failed
//                                     //debug msgId "MESSAGE ACK WRITE FAILURE: %A" e
//                                     logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new SocketListenerException("Failed to respond with Acknowledgement.", listener.LocalEndPoint, e) |> box)
//                             with e -> //forwarding the request to the actor has failed
//                                 //debug msgId "FORWARD TO PRINCIPAL FAILURE: %A" e
//                                 logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new SocketListenerException("Failed to forward message to actor after Acknowledgement.", listener.LocalEndPoint, e) |> box)
//                                 try
//                                     do! protocolStream.AsyncWriteResponse(Failure(msgId, e))
//                                 with e ->
//                                     //debug msgId "MESSAGE FAIL WRITE FAILURE: %A" e
//                                     logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new SocketListenerException("Failed to respond with Failure.", listener.LocalEndPoint, e) |> box)

//                         | Response responseMsg -> 
//                             //debug msgId "PROCESS RESPOSNE"
//                             match responseHandleRegistry.Value.TryFind msgId with
//                             | Some processResponseMessgage -> do! processResponseMessgage protocolStream msgId responseMsg //throws nothing
//                             | None -> do! handleResponseMsg protocolStream msgId responseMsg //this throws nothing
//                     }
//                 | None -> //protocol is configured only for client use
//                     fun msgId msg protocolStream -> async {
//                         match msg with
//                         | Response responseMsg -> 
//                             //debug msgId "PROCESS RESPOSNE"
//                             match responseHandleRegistry.Value.TryFind msgId with
//                             | Some processResponseMessgage -> do! processResponseMessgage protocolStream msgId responseMsg //throws nothing
//                             | None -> do! handleResponseMsg protocolStream msgId responseMsg //this throws nothing
//                         | Request _ ->
//                             logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Received a request for a client side only protocol.") |> box)
//                     }
            
//             //this receives serialized messages from the listener
//             //deserializes and then passes processProtocolMessage for further processing
//             let recipientProcessorBehavior ((msgId, payload, protocolStream): recipientProcessor) = async {
//                 //make sure connection is always properly closed after we are finished
//                 //use _ = protocolStream

//                 try
//                     //debug msgId "MESSAGE PROCESS START"
//                     //second stage deserialization
//                     let msg = serializer.Deserialize<ProtocolMessage<obj>>(payload) |> ProtocolMessage.unbox //throws SerializationException, InvalidCastException

//                     //debug msgId "MESSAGE DESERIALIZED: %A" msg

//                     do! processProtocolMessage msgId msg protocolStream //throws nothing

//                     //debug msgId "MESSAGE PROCESS END"
//                 with e ->
//                     //debug msgId "MESSAGE PROCESS FAILURE: %A" e
//                     //probably an exception related to deserialization
//                     //send with fail
//                     try
//                         do! protocolStream.AsyncWriteResponse(Failure(msgId, e))
//                     with e -> //this again may fail
//                         //debug msgId "MESSAGE FAIL WRITE FAILURE: %A" e 
//                         logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to send a Failure response.", e) |> box)
//             }
//             let recipientProcessor = Actor.bind <| Behavior.stateless recipientProcessorBehavior
            
//             let rec postMessageOnEndPoint (targetEndPoint: IPEndPoint) (msgId: MsgId) (msg: 'T) = 
//                 //let debug x = debug (sprintf "MESSAGE POST::%A" msgId) x
//                 async {
//                     //debug "POSTING MESSAGE %A" msg
//                     //debug "CONNECTING"
//                     try
//                         use! connection = TcpConnectionPool.AsyncAcquireConnection(msgId.ToString(), targetEndPoint)

//                         //do! connection.AsyncConnent targetEndPoint
//                         let stream = connection.GetStream()
//     //                    use protocolStream = new ProtocolStream(msgId, stream, fun () -> sprintfn "CLDISPN %A %A" connection.UnderlyingClient.Client.LocalEndPoint connection.UnderlyingClient.Client.RemoteEndPoint; connection.Return())
//                         use protocolStream = new ProtocolStream(msgId, stream)
//                         //serialize message with the reply patching serialization context
//                         let context = serializationContext listenerAddress
//                         let protocolMessage = serializer.Serialize(Request msg |> ProtocolMessage.box, context.GetStreamingContext())

//                         //handle foreign reply channels provided in the context
//                         let foreignReplyChannelHandlers, foreignReplyChannelsMsgids =
//                             context.ReplyChannelOverrides |> List.map (fun (foreignReplyChannel, nativeReplyChannel) ->
//                                 //the foreign reply channel will not be posted through the message, but the native one will
//                                 //we need to wait for the response on the native rc and forward it to the foreign rc
//                                 //each native reply channel has a new msgId
//                                 let nativeReplyChannel = nativeReplyChannel :?> ReplyChannel
//                                 let awaitResponse = registerAndWaitForResponse (protocolPing targetEndPoint) nativeReplyChannel.MessageId foreignReplyChannel.Timeout
//                                 //if in client mode make sure the recipient processor is registered with the listener
//                                 let listenerRegistration = new ListenerRegistrationResource(clientOnly, actorId, listener, recipientProcessor, recipientProcessorBehavior, ignore)
//                                 async {
//                                     use _ = listenerRegistration //make sure unregistration happens
//                                     //wait for the native rc result
//                                     let! result = awaitResponse
//                                     match result with
//                                     | Some replyResult ->    
//                                         //forward to foreign rc
//                                         try
//                                             foreignReplyChannel.ReplyUntyped(replyResult)
//                                         with e ->
//                                             logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to forward a reply.", e) |> box)
//                                     | None -> ()
//                                         //Either the wait timed-out or it was cancelled.
//                                         //If the wait-timed out on our native rc, then so it will on the foreign rc.
//                                         //If the response wait was cancelled then we failed to post the message.
//                                         //This means that the client posting the message will be notified by an exception.
//                                         //It is the client's responsibility to handle this and perform any action on the
//                                         //foreign rcs. 
//                                         //Thus, we do nothing in this case.
                                
//                                 },
//                                 nativeReplyChannel.MessageId
//                                 //we map to pairs of async reply forward computation and the native rc msgId
//                                 //because if something goes wrong in the message post
//                                 //we need to use the msgIds to unregister the waits
//                             ) |> List.unzip
//                         //start handling the replies for foreign reply channels
//                         foreignReplyChannelHandlers |> Async.Parallel |> Async.Ignore |> Async.Start

//                         //create and write protocol request write message
//                         let protocolRequest = msgId, actorId, protocolMessage : ProtocolRequest

//                         try
//                             //debug "POSTING"
//                             do! protocolStream.AsyncWriteRequest(protocolRequest)
                
//                             //debug "POSTED"

//                             return! protocolStream.AsyncReadResponse()
//                         with e ->
//                             //debug "POST WRITE FAILURE: %A" e
//                             //cancel the async handling the foreign rc responses
//                             foreignReplyChannelsMsgids |> List.iter cancelResponseWait
//                             return! Async.Raise e
//                     with e ->
//                         //debug "POST FAILURE: %A" e
//                         return! Async.Raise e
//                 }

//             let postMessageAndProcessResponse endPoint msgId msg = 
//                 //let debug x = debug (sprintf "MESSAGE POST::%A" msgId) x
//                 async {
//                     let! protocolResponse = attempt (postMessageOnEndPoint endPoint msgId msg)

//                     return match protocolResponse with
//                                  | Acknowledge _ -> 
//                                     //debug "POST SUCCESS"
//                                     Choice1Of2() //post was successfull
//                                  | UnknownRecipient _ ->
//                                      //debug "POST UNKNOWN RECIPIENT"
//                                      Choice2Of2(new UnknownRecipientException("Remote host could not find the message recipient.") :> exn)
//                                  | Failure(_, e) -> 
//                                      //debug "POST FAILURE: %A" e
//                                      Choice2Of2(new CommunicationException("Message delivery failed on the remote host.", e) :> exn)
//                 }

//             let rec postMessageLoop endPoints msgId msg = async {
//                 match endPoints with
//                 | [endPoint] ->
//                     let! r = postMessageAndProcessResponse endPoint msgId msg
//                              |> Async.Catch
//                     match r with
//                     | Choice1Of2 r' ->
//                         match r' with
//                         | Choice1Of2 _ -> return ()
//                         | Choice2Of2 e -> return! raisex e
//                     | Choice2Of2 e -> return! raisex e
//                 | endPoint::rest ->
//                     let! r = postMessageAndProcessResponse endPoint msgId msg
//                              |> Async.Catch
//                     match r with
//                     | Choice1Of2 r' ->
//                         match r' with
//                         | Choice1Of2 _ -> return ()
//                         | Choice2Of2 _ ->
//                             return! postMessageLoop rest msgId msg
//                     | Choice2Of2 _ ->
//                         return! postMessageLoop rest msgId msg
//                 | _ -> return! raisex (new SystemException("utcp :: postMessageLoop :: Invalid State :: target endpoints exhausted"))
//             }

//             let postMessage msgId msg = async {
//                 let! endPoints = address.ToEndPointsAsync()
//                 return! postMessageLoop endPoints msgId msg
//             }

//             let postMessageWithReplyOnEndPoint (targetEndPoint: IPEndPoint) (msgId: MsgId) (msg: 'T) (timeout: int) = 
//                 //let debug x = debug (sprintf "MESSAGE POST::%A" msgId) x
//                 async {
//                     //if in client mode make sure the recipient processor is registered with the listener
//                     Atom.swap responseHandleRegistry (Map.add msgId handleResponseMsg)
//                     //use _ = new ListenerRegistrationResource(clientOnly, actorId, listener, recipientProcessor, (fun () -> Atom.swap responseHandleRegistry (Map.remove msgId)))
//                     use _ = if not (listener.IsrecipientRegistered actorId) then
//                                 listener.RegisterMessageProcessor(msgId, recipientProcessorBehavior)
//                                 { new IDisposable with override __.Dispose() = Atom.swap responseHandleRegistry (Map.remove msgId); listener.UnregisterMessageProcessor(msgId) }
//                             else { new IDisposable with override __.Dispose() = Atom.swap responseHandleRegistry (Map.remove msgId) }

//                     let awaitResponse = registerAndWaitForResponse (protocolPing targetEndPoint) msgId timeout

//                     let! r = postMessageAndProcessResponse targetEndPoint msgId msg
//                              |> Async.Catch
//                     match r with
//                     | Choice1Of2 r' ->
//                         match r' with
//                         | Choice1Of2 _ ->
//                             //debug "WAITING FOR REPLY"
//                             let! response = Async.Catch awaitResponse
//                             match response with
//                             | Choice1Of2 r ->
//                                 //debug "REPLY RETURNED"
//                                 return Choice1Of2 r
//                             | Choice2Of2 e -> return Choice2Of2 e
//                         | Choice2Of2 e ->
//                             unregisterResponseAsyncWait msgId
//                             return Choice2Of2 e
//                     | Choice2Of2 e ->
//                         unregisterResponseAsyncWait msgId
//                         return Choice2Of2 e
//                 }

//             let rec postMessageWithReplyLoop endPoints msgId msg timeout = async {
//                 match endPoints with
//                 | [endPoint] ->
//                     let! r = postMessageWithReplyOnEndPoint endPoint msgId msg timeout
//                     match r with
//                     | Choice1Of2 reply -> return reply
//                     //| Choice2Of2(CommunicationException(_, _, _, InnerException e)) -> return! raisex e
//                     | Choice2Of2((CommunicationException _) as e) -> return! raisex e
//                     | Choice2Of2 e -> return! raisex <| new CommunicationException("A communication error occurred.", e)
//                 | endPoint::rest ->
//                     let! r = postMessageWithReplyOnEndPoint endPoint msgId msg timeout
//                     match r with
//                     | Choice1Of2 reply -> return reply
//                     | Choice2Of2 _ ->
//                         return! postMessageWithReplyLoop rest msgId msg timeout
//                 | _ -> return! raisex (new SystemException("utcp :: postMessageWithReplyLoop :: Invalid State :: target endpoints exhausted"))
//             }

//             let postMessageWithReply (msgId: MsgId) (msg: 'T) (timeout: int) = async {
//                 let! endPoints = address.ToEndPointsAsync()
//                 return! postMessageWithReplyLoop endPoints msgId msg timeout
//             }

//             //client+server use constructor
//             new (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, actorRef: ActorRef<'T>, ?serializerName: string) =
//                 new Protocol<'T>(actorUUID, actorName, addressKind, Some actorRef, ?serializerName = serializerName)

//             //client only use constructor
//             new (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, ?serializerName: string) =
//                 new Protocol<'T>(actorUUID, actorName, addressKind, None, ?serializerName = serializerName)

//             member private __.Listener = listener

//             //main post message with reply, timeout returns None
//             member private p.TryPostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, ?timeoutOverride: int): Async<'R option> = async {
//                 let msgId = MsgId.NewGuid()

//                 //TODO!!! Make reply channel always have a serializer name. See other todos/notes.
//                 let replyChannel = new ReplyChannel<'R>(listenerAddress, actorId, msgId, serializer = serializerName)

//                 //we always wrap the reply channel in the rc proxy so that it can be considered as a foreign rc
//                 //by other protocols
//                 let msg = msgBuilder (new ReplyChannelProxy<'R>(replyChannel))
//                 //override timeout set on the reply channel, if timeoutOverride is set
//                 timeoutOverride |> Option.iter (fun timeout -> replyChannel.Timeout <- timeout)

//                 let! result = postMessageWithReply msgId msg replyChannel.Timeout
//                 return result |> Option.map Reply.unbox<'R>
//                               |> Option.map (function Value v -> v | Exception e -> raise (new MessageHandlingException("Remote Actor threw exception while handling message.", actorName, actorUUID, e)))
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
//                     new UTcp(ProtocolAddressKind.toPublishMode addressKind, serializerName) :> IProtocolConfiguration |> Some

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

//                 member p.PostAsync(msg: 'T) = async {
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
//                         listenerLogSubcription <- listener.Log //|> Observable.choose (function Warning, source, (:? SocketResponseException as e) when e.ActorId = actorId -> Some(Warning, source, e :> obj) | _ -> None)
//                                                   |> Observable.subscribe logEvent.Trigger
//                                                   |> Some

//                         recipientProcessor.Start()
//                         listener.Registerrecipient(actorId, !recipientProcessor, recipientProcessorBehavior)
//                         ServerSideProtocolPool.Register(actorId, p)
//                     | _ -> ()

//                 member p.Stop() =
//                     match listenerLogSubcription with
//                     | Some disposable ->
//                         ServerSideProtocolPool.UnRegister(actorId)
//                         listener.Unregisterrecipient(actorId)
//                         recipientProcessor.Stop()
//                         disposable.Dispose()
//                         listenerLogSubcription <- None
//                     | None -> ()
