namespace Nessos.Thespian.Remote.TcpProtocol

    open System
    open System.IO
    open System.Net
    open System.Net.Sockets
    open System.Threading
    open System.Threading.Tasks
    open System.Runtime.Serialization

    open Nessos.Thespian
    open Nessos.Thespian.AsyncExtensions
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Utils
    open Nessos.Thespian.DisposableExtensions
    open Nessos.Thespian.Remote
    open Nessos.Thespian.Remote.SocketExtensions
    open Nessos.Thespian.Remote.ConnectionPool

    module Bidirectional =
        let ProtocolName = "btcp"

        let private NullSerializationContext = obj()

        let rec private attempt f = async {
            try
                return! f
            with :? SocketException as e when e.SocketErrorCode = SocketError.ConnectionReset || e.SocketErrorCode = SocketError.ConnectionAborted ->
                    return! attempt f
                | :? EndOfStreamException -> return! attempt f
                | :? IOException as e -> match e.InnerException with
                                         | :? SocketException as e' when e'.SocketErrorCode = SocketError.ConnectionReset || e'.SocketErrorCode = SocketError.ConnectionAborted ->
                                                return! attempt f
                                         | _ -> 
                                            //Debug.writelf "IOException %A" e
                                            return! Async.Raise e
        }
        
        type ProtocolMessageStream(protocolStream: ProtocolStream, serializer: IMessageSerializer) =
            let sync = obj()
            let isDisposed = ref true
            let disposeCont = ref (None : (unit -> unit) option)
            let cancelCont = ref (None: (OperationCanceledException -> unit) option)

            member __.ProtocolStream = protocolStream

            member __.AsyncWriteProtocolMessage(msgId: MsgId, actorId: TcpActorId, protocolMessage: 'T, ?serializationContext: MessageSerializationContext) =
                async {
                    let context = serializationContext |> Option.map (fun c -> c.GetStreamingContext())
                    let serializedProtocolMessage = serializer.Serialize<obj>(protocolMessage, ?context = context)
                    let protocolRequest = msgId, actorId, serializedProtocolMessage : ProtocolRequest
                    return! protocolStream.AsyncWriteRequest(protocolRequest)
                }

            member __.Dispose() = 
                lock sync (fun () ->
                    match disposeCont.Value with
                    | Some cont -> 
                            cont()
                    | None -> isDisposed := true
                )

            member __.Acquire() = isDisposed := false

            member __.AsyncWaitForDisposal() = async {
                use! d = Async.OnCancel (fun () -> lock sync (fun () -> match cancelCont.Value with Some cancel -> cancel (new OperationCanceledException()) | _ -> ()))
                return! Async.FromContinuations(fun (success, _, cancel) ->
                    lock sync (fun () ->
                        if isDisposed.Value then
                            success()
                        else 
                            disposeCont := Some success
                            cancelCont := Some cancel
                    )
                )
            }

            override __.GetHashCode() = protocolStream.GetHashCode()
            override __.Equals(other: obj) =
                match other with
                | :? ProtocolMessageStream as otherStream -> protocolStream.Equals(otherStream.ProtocolStream)
                | _ -> false

            interface IComparable with
                member __.CompareTo(other: obj) =
                    match other with
                    | :? ProtocolMessageStream as pms -> compare protocolStream pms.ProtocolStream
                    | :? ProtocolStream as ps -> compare protocolStream ps
                    | _ -> invalidArg "other" "Cannot compare values of incompatible types."

            interface IDisposable with
                member s.Dispose() = s.Dispose()


        type AsyncExecutor = IReplyChannel<unit> * Async<unit>

        let private asyncExecutorBehavior = 
            Behavior.stateless (fun (R(reply), computation) -> async {
                try
                    do! computation //perform computation
                    reply nothing //say when finished
                with e -> //something went wrong with the reply
                    reply <| Exception e
            })

        type DeserializationContext(stream: ProtocolMessageStream) =
            member c.ProtocolMessageStream = stream
            member c.ReplyWriter: Actor<AsyncExecutor> = Actor.bind asyncExecutorBehavior |> Actor.start
            member c.GetStreamingContext() = new StreamingContext(StreamingContextStates.All, c)


        type internal AsyncCountdownLatch(initialCount: int) =
            let mutable count = initialCount
            let continuation: (unit -> unit) option ref = ref None
            
            member __.Set(initialCount: int) = count <- initialCount

            member __.Decrement() =
                if Interlocked.Decrement(&count) = 0 then
                    match continuation.Value with
                    | Some success -> success()
                    | _ -> ()

            member __.AsyncWaitToZero() =
                Async.FromContinuations(fun (success, _, _) -> 
                    if Interlocked.CompareExchange(&count, 0, 0) <> 0 then continuation := Some success else success())
                


        type ReplyChannel = Unidirectional.ReplyChannel

        [<Serializable>]
        type ReplyChannel<'T> =
            inherit ReplyChannel
        
            //val private streamResource: NestedDisposable<ProtocolStream>
            val private stream: NestedDisposable<ProtocolMessageStream>
            val private replyWriterResource: NestedDisposable<Actor<AsyncExecutor>>
            
            new (actorId: TcpActorId, msgId: MsgId, ?serializer: string) = {
                inherit ReplyChannel(actorId, msgId, ?serializer = serializer)
                //stream is set to null because this constructor is only used at client side
                //streamResource = Unchecked.defaultof<NestedDisposable<ProtocolStream>>
                stream = Unchecked.defaultof<NestedDisposable<ProtocolMessageStream>>
                replyWriterResource = Unchecked.defaultof<NestedDisposable<Actor<AsyncExecutor>>>
            }

            internal new (info: SerializationInfo, context: StreamingContext) = 
                let stream, replyWriter = match context.Context with
                                          | :? DeserializationContext as deserializationContext -> 
                                            deserializationContext.ProtocolMessageStream.Acquire()
                                            deserializationContext.ProtocolMessageStream, deserializationContext.ReplyWriter
                                          | _ -> invalidArg "Invalid deserialization context given." "context"
                {
                    inherit ReplyChannel(info, context)
                    //streamResource = useNested stream.ProtocolStream //useNested to increase ref count on the disposable
                    stream = useNested stream
                    replyWriterResource = useNested replyWriter
                }

            member r.ProtocolMessageStream = 
                //if r.streamResource <> Unchecked.defaultof<NestedDisposable<ProtocolStream>> then r.streamResource.Resource
                if r.stream <> Unchecked.defaultof<NestedDisposable<ProtocolMessageStream>> then r.stream.Resource.ProtocolStream
                else invalidOp "ProtocolMessageStream is not accessible from client side code."
                //r.stream

            member r.ReplyWriter = 
                if r.replyWriterResource <> Unchecked.defaultof<NestedDisposable<Actor<AsyncExecutor>>> then r.replyWriterResource.Resource
                else invalidOp "ReplyWriter is not accessible from client side code."

            interface IReplyChannel with
                member ch.Protocol = ProtocolName
                member ch.Timeout with get() = ch.Timeout and set(timeout': int) = ch.Timeout <- timeout'    
                member ch.ReplyUntyped(reply: Reply<obj>) =
                    (ch :> IReplyChannel<'T>).Reply(match reply with Value v -> Value(v :?> 'T) | Exception e -> Exception(e) : Reply<'T>)

            member private r.AsyncReply(reply: Reply<'T>) = async {
                use _ = r.stream //we use this so that the ref count on the disposable is decreased
                //use _ = r.streamResource

                let! protocolResponse = async {
                    try
                        //sprintfn "RPL %A" r.MessageId
                        //we box the reply; the client may be expecting more than one reply in varied orders
                        //see foreign vs native reply channels
                        do! r.stream.Resource.AsyncWriteProtocolMessage(r.MessageId, r.ActorId, Reply.box reply)

                        //sprintfn "RPLWRT %A" r.MessageId

                        let! response = r.stream.Resource.ProtocolStream.AsyncReadResponse()

                        //sprintfn "RPLRSP %A" r.MessageId
                        return response
                    with e ->
                        //sprintfn "RPLRSPEXN %A %A" r.MessageId e
                        return raise <| new CommunicationException("Unable to send reply to sender.", e)
                }
                match protocolResponse with
                | Acknowledge _ -> () //sprintfn "RPL OK %A" r.MessageId; () //all went ok
                | UnknownRecipient(msgId, actorId) -> raise <| new UnknownRecipientException("Recipient of reply not found on remote host.")
                | Failure(_, e) -> raise <| new CommunicationException("Delivery of reply failed on remote host.", e)
            }

            interface IReplyChannel<'T> with
                member r.WithTimeout(timeout: int) = r.Timeout <- timeout; r :> IReplyChannel<'T>

                member r.Reply(reply: Reply<'T>) = 
                    //there may be many concurrent replies, all using the same connection
                    //one reply is not an atomic operation;
                    //it consists of two operations: send reply, receive acknowledgement
                    //we use a reply writer, an actor that executes async computations
                    //thus the replies will be serialized
                    //the reply writer is used in a NestedDisposable, so that when all
                    //the replies are finished, the actor is disposed
                    use rw = r.replyWriterResource
                    try
                        r.ReplyWriter.Ref <!= fun ch -> ch, r.AsyncReply(reply)
                    //as the reply computation is executed in the actor,
                    //any exception will be carried inside a MessageHandlingException
                    //so we unwrap
                    with MessageHandlingException(_, _, _, e) -> raise e
                         | :? ActorInactiveException -> raise <| new CommunicationException("Attempting to reply a second time on a closed channel.")

            interface ISerializable with
                member r.GetObjectData(info: SerializationInfo, context: StreamingContext) = 
                    base.GetObjectData(info, context)

        
        [<Serializable>]
        type BTcp = 
            inherit TcpProtocolConfiguration 

            new (publishMode: PublishMode, ?serializerName: string) = {
                inherit TcpProtocolConfiguration(publishMode, ?serializerName = serializerName)
            }

            internal new (info: SerializationInfo, context: StreamingContext) = {
                inherit TcpProtocolConfiguration(info, context)
            }

            //server side constructor
            //auto-select address; the addess selected will be detrmined by TcpListenerPool.Hostname
            new () = new BTcp(Publish.all)
            //server side constructor
            //specific port; the address will be detrmined by TcpListenerPool.Hostname
            new (port: int, ?serializerName: string) = new BTcp(Publish.endPoints [IPEndPoint.anyIp port], ?serializerName = serializerName)
            
            override tcp.ProtocolName = ProtocolName
            override tcp.ConstructProtocols<'T>(actorUUId: ActorUUID, actorName: string, publishMode: PublishMode, actorRef: ActorRef<'T> option, serializerName: string option) =
                match publishMode with
                | Client addresses ->
                    addresses |> List.map (fun addr -> new Protocol<'T>(actorUUId, actorName, ClientAddress addr, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T>)
                              |> List.toArray
                | Server [] ->
                    [| for listener in TcpListenerPool.GetListeners(IPEndPoint.any, ?serializer = serializerName) ->
                            new Protocol<'T>(actorUUId, actorName, ServerEndPoint listener.LocalEndPoint, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T> |]
                | Server ipEndPoints ->
                    [| for ipEndPoint in ipEndPoints ->
                            new Protocol<'T>(actorUUId, actorName, ServerEndPoint ipEndPoint, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T> |]

        //if actorRef is Some then the protocol instance is used in server+client mode
        //if None it is in client only mode
        and Protocol<'T> internal (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, actorRef: ActorRef<'T> option, ?serializerName: string) =
            let serializerName = serializerNameDefaultArg serializerName
            let serializer = 
                if SerializerRegistry.IsRegistered serializerName then 
                    SerializerRegistry.Resolve serializerName
                else invalidArg "Unknown serializer." "serializerName"

            let clientOnly = actorRef.IsNone

            //we do not always need a listener
            //only in server/client mode
            let listener = 
                match actorRef with
                | Some _ ->
                    match addressKind with
                    | ClientAddress _ -> None
                    | ServerEndPoint endPoint -> TcpListenerPool.GetListener(endPoint, serializer = serializerName) |> Some
                | None -> None

            let mutable listenerLogSubcription: IDisposable option = None
            
            let targetAddress =
                if clientOnly && ProtocolAddressKind.isServerEndPoint addressKind then invalidArg "protocolAddressKind" "Need to specify address in client only mode."
                match addressKind with
                | ClientAddress address -> address
                | ServerEndPoint endPoint -> new Address(TcpListenerPool.DefaultHostname, endPoint.Port)

            //let debug i x = Debug.writelfc (sprintf "btcp(%A)::%A" targetAddress i) x

            let actorId = new TcpActorId(actorUUID, actorName, ProtocolName, targetAddress)

            let logEvent = new Event<Log>()

            //response waiting registry
            //we use an AsyncResultCell to wait for a response
            //a response is either a Reply, in which case a proper reply has been received,
            //or not a reply, in which case Some(exn) indicates a communication failure
            //while None indicates a timeout or cancellation
            let registeredResponseAsyncWaits = Atom.atom Map.empty<MsgId, AsyncResultCell<Choice<Reply<obj>, exn option>>>
            //unregister a wait for response
            let unregisterResponseAsyncWait msgId = Atom.swap registeredResponseAsyncWaits (fun regs -> Map.remove msgId regs)
            //register and get an async that waits for a response
            let registerAndWaitForResponse msgId timeout =
                let resultCell = new AsyncResultCell<_>()

                Atom.swap registeredResponseAsyncWaits (fun regs -> Map.add msgId resultCell regs)

                async {
                    let! result = resultCell.AsyncWaitResult(timeout) //throws ResponseAwaitCancelled
                    unregisterResponseAsyncWait msgId //always unregister in the end
                    return match result with
                           | Some r -> r
                           | None -> Choice2Of2 None
                }
            //cancel waiting for a response, cancelling causes the same result as timeout
            let cancelResponseWait msgId = 
                registeredResponseAsyncWaits.Value.TryFind(msgId) |> Option.iter (fun resultCell -> resultCell.RegisterResult(Choice2Of2 None))
            //fail waiting for a resposne
            let failResponseWait e msgId =
                 registeredResponseAsyncWaits.Value.TryFind(msgId) |> Option.iter (fun resultCell -> resultCell.RegisterResult(Choice2Of2(Some e)))

            //this receives serialized messages from the listener
            //deserializes and then passes processProtocolMessage for further processing
            let recepientProcessorBehavior (actorRef: ActorRef<'T>) ((msgId, payload, protocolStream): RecepientProcessor) = 
                async {
                    //debug msgId "MESSAGE PROCESS START"

                    let protocolMessageStream = new ProtocolMessageStream(protocolStream, serializer)

                    let deserializationResult = 
                        try
                            //second stage deserialization
                            //if during the deserialization we encounter a reply channel, then the ProtocolMessageStream
                            //is going to be useNested, preventing its disposal until all reply channels have been replied to
                            //throws SerializationException
                            let context = new DeserializationContext(protocolMessageStream)
                            let msg = serializer.Deserialize<obj>(payload, context.GetStreamingContext()) :?> 'T
                            Choice1Of2 msg
                        with e -> Choice2Of2 e

                    try
                        match deserializationResult with
                        | Choice1Of2 msg ->
                            //sprintfn "DSRL %A" msgId
                            //send ack back
                            do! protocolStream.AsyncWriteResponse(Acknowledge msgId)
                            //if the above fails, we do not want to continue with the message
                            //because if it is a two-way message the connection will be gone    

                            //debug msgId "FORWARD TO PRINCIPAL"

                            //forward message to actor
                            actorRef <-- msg

                            //debug msgId "WAITING FOR PROTOCOL MESSAGE STREAM DISPOSAL"
//                            try
                            //NOTE!!! This is wrong
                            //On timeout, we should invalidate all reply channels
                            //because it might take more than the timeout to respond
                            let! r = protocolMessageStream.AsyncWaitForDisposal() |> Async.WithTimeout 120000
//                            do! protocolMessageStream.AsyncWaitForDisposal() |> Async.WithTimeout 120000 |> Async.Ignore
                            match r with
                            | Some _ -> ()
                            | None -> protocolStream.Retain <- false  //on timeout make the listener reset the connection
//                            with _ ->
//                                protocolStream.Dispose()
                            //protocolStream.Retain <- false
                        | Choice2Of2 e ->
                            //sprintfn "DSRLFAULT %A %A" msgId e
                            //probably an exception related to deserialization
                            //send with fail
                            do! protocolStream.AsyncWriteResponse(Failure(msgId, e))
                            //protocolStream.Dispose()
                            //protocolStream.Retain <- false

                        //debug msgId "MESSAGE PROCESS END"
                    with e ->
                        //debug msgId "ACKNOWLEDGE WRITE FAILURE: %A" e
                        //writing back protocol resposne failure
                        logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to write protocol response.", e) |> box)
                        //protocolStream.Dispose()
                        //protocolStream.Retain <- false
                }

            let recepientProcessor = 
                match actorRef with
                | Some actorRef -> Actor.bind <| Behavior.stateless (recepientProcessorBehavior actorRef)
                | None -> Actor.sink() //if in client only mode, we are not getting messages from the tcp listener

            let newSerializationContext() =
                new MessageSerializationContext(serializer,
                    {
                        new IReplyChannelFactory with 
                            member f.Protocol = ProtocolName
                            member f.Create<'R>() = 
                                let newMsgId = MsgId.NewGuid()
                                new ReplyChannelProxy<'R>(
                                    new ReplyChannel<'R>(actorId, newMsgId, serializerName)
                                )
                    })

            let processReply (protocolStream: ProtocolStream) = async {
                //debug 0 "WAITING FOR REPLY"
                let! replyMsgId, actorId, serializedReply = protocolStream.AsyncReadRequest() //if this fails, then the connection fails

                let deserializationResult =
                    try
                        //can fail
                        let reply = serializer.Deserialize<obj>(serializedReply) :?> Reply<obj>
                        Choice1Of2 reply
                    with e -> Choice2Of2 e

                match deserializationResult, registeredResponseAsyncWaits.Value.TryFind replyMsgId with
                | Choice1Of2 reply, Some responseAsyncCell -> //found a waiting reply
                    //debug replyMsgId "REPLY RECEIVED"
                    //send back ack
                    do! protocolStream.AsyncWriteResponse(Acknowledge(replyMsgId)) //if this fails, connection fails

                    //set reply result
                    try
                        responseAsyncCell.RegisterResult(Choice1Of2 reply)
                        //debug replyMsgId "REPLY REGISTERED"
                    with e ->
                        //debug replyMsgId "REPLY RESULT REG FAILURE: %A" e
                        //setting the result has failed
                        logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Setting the result for a reply has failed after responding with Acknowledgement.", e) |> box)
                | Choice1Of2 _, None -> //no matching waiting reply found
                    //debug replyMsgId "REPLY RECEIVE UNKNOWN MSG"
                    //send back unknown recipient
                    do! protocolStream.AsyncWriteResponse(UnknownRecipient(replyMsgId, actorId)) //if this fails, then the connection fails
                | Choice2Of2 e, _ ->
                    //debug replyMsgId "REPLY RECEIVE DESERIALIZATION FAILURE: %A" e
                    do! protocolStream.AsyncWriteResponse(Failure(replyMsgId, e)) //if this fails, then the connection fails
            }

            let postMessageOnEndPoint (targetEndPoint: IPEndPoint) (msgId: MsgId) (msg: 'T) (timeoutResource: AsyncCountdownLatch option) = async {
                //let debug x = debug msgId x
                let withReply = timeoutResource.IsSome
                //if withReply then debug "POSTING MESSAGE WITH REPY %A" msg else debug "POSTING MESSAGE %A" msg

                //debug "CONNECTING"
                let! connection = TcpConnectionPool.AsyncAcquireConnection targetEndPoint

                let stream = connection.GetStream() //stream.Close(); connection.UnderlyingClient.Close(); 
//                let protocolStream = new ProtocolStream(msgId, stream, fun () -> sprintfn "CLDISPN %A %A" connection.UnderlyingClient.Client.LocalEndPoint connection.UnderlyingClient.Client.RemoteEndPoint; connection.Return())
                let protocolStream = new ProtocolStream(msgId, stream)
                use _ = useNested protocolStream

                let serializationContext = newSerializationContext()
                let serializedMessage = serializer.Serialize<obj>(msg, serializationContext.GetStreamingContext())

                //handle foreign reply channels provided in the context
                //We will async start one computation waiting for a reply for each reply channel override
                //Based on the msgId of the reply on a native rc we find the foreign rc and forward it
                let foreignReplyChannelMap = 
                    serializationContext.ReplyChannelOverrides |> List.map (fun (foreignReplyChannel, nativeReplyChannel) ->
                        let nativeReplyChannel = nativeReplyChannel :?> ReplyChannel
                        nativeReplyChannel.MessageId, foreignReplyChannel
                    ) |> Map.ofList

                let timeoutResource = if withReply then timeoutResource.Value else new AsyncCountdownLatch(0)

                //handle foreign reply channels provided in the context
                let foreignReplyChannelHandlers, foreignReplyChannelMsgIds =
                    serializationContext.ReplyChannelOverrides |> List.map (fun (foreignReplyChannel, nativeReplyChannel) ->
                        //the foreign reply channel will not be posted through the message, but the native one will
                        //we need to wait for the response on the native rc and forward it to the foreign rc
                        //each native reply channel has a new msgId
                        let nativeReplyChannel = nativeReplyChannel :?> ReplyChannel
                        let awaitResponse = registerAndWaitForResponse nativeReplyChannel.MessageId foreignReplyChannel.Timeout
                        async {
                            //wait for the native rc result
                            let! result = awaitResponse
                            match result with
                            | Choice1Of2 replyResult ->    
                                //forward to foreign rc
                                try
                                    foreignReplyChannel.ReplyUntyped(replyResult)
                                with e ->
                                    logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to forward a reply.", e) |> box)
                            | Choice2Of2(Some e) ->
                                //A communication failure occurred.
                                //forward to foreign rc
                                try
                                    foreignReplyChannel.ReplyUntyped(Exception(new CommunicationException("A communication failure occurred while waiting for reply.", e)))
                                with e ->
                                    logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to forward a reply.", e) |> box)
                            | Choice2Of2 None -> ()
                                //Either the wait timed-out or it was cancelled.
                                //If the wait-timed out on our native rc, then so it will on the foreign rc.
                                //If the response wait was cancelled then we failed to post the message.
                                //This means that the client posting the message will be notified by an exception.
                                //It is the client's responsibility to handle this and perform any action on the
                                //foreign rcs. 
                                //Thus, we do nothing in this case.

                            timeoutResource.Decrement()
                                
                        },
                        nativeReplyChannel.MessageId
                        //we map to pairs of async reply forward computation and the native rc msgId
                        //because if something goes wrong in the message post
                        //we need to use the msgIds to unregister the waits
                    ) |> List.unzip
                //start handling the replies for foreign reply channels
                foreignReplyChannelHandlers |> Async.Parallel |> Async.Ignore |> Async.Start

                //post the message
                //create and write protocol request write message
                let protocolRequest = msgId, actorId, serializedMessage : ProtocolRequest

                try
                    //debug "POSTING"
                    do! protocolStream.AsyncWriteRequest(protocolRequest)
                    //debug "POSTED"
                    let! response = protocolStream.AsyncReadResponse()
                    
                    match response with
                    | Acknowledge _ -> () //debug "POST SUCCESS" //post was successfull
                    | UnknownRecipient _ -> 
                        //debug "POST UNKNOWN RECIPIENT"
                        raise <| new UnknownRecipientException("Remote host could not find the message recipient.")
                    | Failure(_, e) -> 
                        //debug "POST FAILURE: %A" e
                        raise <| new CommunicationException("Message delivery failed on the remote host.", e)

                    //start reply processing
                    let expectedReplies = foreignReplyChannelMap.Count + if withReply then 1 else 0
                    timeoutResource.Set(expectedReplies)
                    //hold on to the connection until all replies have been dealt with
                    //the protocol stream has expectedReplies ref count
                    let protocolStreamResource = useNested protocolStream
                    async {
                        use _ = protocolStreamResource
                        try
                            let replyProcessing = async {
                                for _ in 1..expectedReplies do
                                    do! processReply protocolStream //this will fail if connection fails
                            }
                            let timeoutResourceWait = async {
                                do! timeoutResource.AsyncWaitToZero()
                                return true
                            }
                            //debug "PROCESSING REPLIES"
                            do! Async.ConditionalCancel timeoutResourceWait replyProcessing |> Async.Ignore
                            //debug "REPLIES PROCESSED"
                        with e -> //connection failure
                            //debug "REPLY PROCESSING FAILURE: %A" e
                            logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Communication failure while waiting for reply.", e) |> box)
                            //need to cancel all oustanding reply waits
                            foreignReplyChannelMsgIds |> List.iter (failResponseWait e)
                            if withReply then failResponseWait e msgId
                    } |> Async.Start

                with e ->
                    //debug "POST FAILURE: %A" e
                    let le, re = if connection.UnderlyingClient.Client <> null then connection.UnderlyingClient.Client.LocalEndPoint, connection.UnderlyingClient.Client.RemoteEndPoint else null, null
                    //either writing the protocol message, or reading the protocol response failed
                    //due to connection failure, or due to malformed messaging
                    //or unboxing fails due to client-server type mismatch
                    //in any case, we want to cancel waiting for any responses on overriden
                    //foreign reply channels
                    //----
                    //cancel the async handling the foreign rc responses
                    foreignReplyChannelMsgIds |> List.iter cancelResponseWait
                    protocolStream.Dispose()

                    return! raisex e
            }

            let postMessageGenericLoop (msgId: MsgId) (msg: 'T) (timeoutResource: AsyncCountdownLatch option) = async {
                let! endPoints = targetAddress.ToEndPointsAsync()

                return! postMessageOnEndPoint endPoints.[0] msgId msg timeoutResource
            }

            let postMessage msgId msg = attempt (postMessageGenericLoop msgId msg None)

            let postMessageWithReply (msgId: MsgId) (msg: 'T) (timeout: int) = async {
                //register and get async wait for response
                let awaitResponse = registerAndWaitForResponse msgId timeout
                
                let timeoutResource = new AsyncCountdownLatch(0)

                //post message
                try
                    do! attempt (postMessageGenericLoop msgId msg (Some timeoutResource))
                with CommunicationException(_, _, _, InnerException e) -> 
                        unregisterResponseAsyncWait msgId
                        timeoutResource.Decrement()
                        raise e
                    | (CommunicationException _) as e ->
                        unregisterResponseAsyncWait msgId
                        timeoutResource.Decrement()
                        raise e
                    | e -> 
                        //if smth goes wrong in the post we must unregister the awaitResponse
                        //we are not cancelling the resposne wait here because we are not waiting
                        //for it yet
                        unregisterResponseAsyncWait msgId
                        timeoutResource.Decrement()
                        //and reraise the exception
                        raise <| new CommunicationException("A communication error occurred.", e)
                
                //async wait for resposne and return
                let! responseResult = awaitResponse
                timeoutResource.Decrement()
                return match responseResult with
                       | Choice1Of2 replyResult -> Some replyResult //normal result
                       | Choice2Of2(Some(CommunicationException(_, _, _, e))) -> raise e //Communication failure
                       | Choice2Of2(Some e) -> raise <| new CommunicationException("A communication error occurred.", e) //Communication failure
                       | Choice2Of2 None -> None //timeout
            }

            //client+server use constructor
            new (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, actorRef: ActorRef<'T>, ?serializerName: string) =
                new Protocol<'T>(actorUUID, actorName, addressKind, Some actorRef, ?serializerName = serializerName)

            //client only use constructor
            new (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, ?serializerName: string) =
                new Protocol<'T>(actorUUID, actorName, addressKind, None, ?serializerName = serializerName)

            //main post message with reply, timeout returns None
            member private p.TryPostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, ?timeoutOverride: int): Async<'R option> = async {
                let msgId = MsgId.NewGuid()

                let replyChannel = new ReplyChannel<'R>(actorId, msgId, serializer = serializerName)

                let msg = msgBuilder (new ReplyChannelProxy<'R>(replyChannel))
                //override timeout set on the reply channel, if timeoutOverride is set
                timeoutOverride |> Option.iter (fun timeout -> replyChannel.Timeout <- timeout)

                let! result = postMessageWithReply msgId msg replyChannel.Timeout
                return result |> Option.map Reply.unbox<'R>
                              |> Option.map (function Value v -> v | Exception e -> raise (new MessageHandlingException("Remote Actor threw exception while handling message", actorName, actorUUID, e)))
            }
            //post with reply, timeout throws exception
            member private p.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, ?timeoutOverride: int): Async<'R> = async {
                let! result = p.TryPostWithReply(msgBuilder, ?timeoutOverride = timeoutOverride)
                return match result with
                        | Some v -> v
                        | None -> raise <| new TimeoutException("Waiting for the reply has timed-out.")
            }

            override p.ToString() =
                sprintf "%s://%O.%s" ProtocolName actorId typeof<'T>.Name

            interface IActorProtocol<'T> with

                //TODO!!! Change this so that the configuration always has a serializer name,
                //even in the case of the default one
                //NOTE!!! When using a default serializer, the serializer name is ""
                //We should be able to get the serializer name from the serializer object
                member p.Configuration =
                    new BTcp(ProtocolAddressKind.toPublishMode addressKind, serializerName) :> IProtocolConfiguration |> Some

                member p.MessageType = typeof<'T>

                member p.ProtocolName = ProtocolName
                
                //TODO!!! Check the correctness of these
                //this is different behavior and perhaps not the one intended
                //need to check the repercutions
                member p.ActorUUId = actorUUID
                member p.ActorName = actorName
                member p.ActorId = actorId :> ActorId

                member p.Log = logEvent.Publish

                //Post a message
                member p.Post(msg: 'T) =
                    (p :> IActorProtocol<'T>).PostAsync(msg) |> Async.RunSynchronously

                member p.PostAsync(msg: 'T): Async<unit> = async {
                    let msgId = MsgId.NewGuid()
                    try
                        do! postMessage msgId msg
                    with (CommunicationException _) as e -> return raise e
                         | e -> return raise <| new CommunicationException("A communication error occurred.", e)
                }

                //TODO!!! Fix IActorProtocols and ActorRefs to have this method with an optional timeout
                //like our method outside the iface
                member p.TryPostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, timeout: int): Async<'R option> = 
                    p.TryPostWithReply(msgBuilder, timeout)

                //Post a message and wait for reply with infinite timeout
                member p.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T): Async<'R> =
                    p.PostWithReply(msgBuilder)

                //Post a message and wait for reply with specified timeout, 
                member p.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, timeout: int): Async<'R> =
                    p.PostWithReply(msgBuilder, timeout)

                member p.Start() = 
                    match listenerLogSubcription with
                    | None ->
                        //TODO!!! Fix this
                        listener |> Option.iter (fun listener ->
                            listenerLogSubcription <- listener.Log //|> Observable.choose (function Warning, source, (:? SocketResponseException as e) when e.ActorId = actorId -> Some(Warning, source, e :> obj) | _ -> None)
                                                                   |> Observable.subscribe logEvent.Trigger
                                                                   |> Some)

                        recepientProcessor.Start()
                        listener |> Option.iter (fun listener -> listener.RegisterRecepient(actorId, !recepientProcessor, match actorRef with Some actorRef -> recepientProcessorBehavior actorRef | None -> fun _ -> async.Return()))
                    | _ -> ()

                member p.Stop() =
                    match listenerLogSubcription with
                    | Some disposable ->
                        listener |> Option.iter (fun listener -> listener.UnregisterRecepient(actorId))
                        recepientProcessor.Stop()
                        disposable.Dispose()
                        listenerLogSubcription <- None
                    | None -> ()

