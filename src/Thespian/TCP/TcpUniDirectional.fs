module Nessos.Thespian.Remote.TcpProtocol.Uniderctional

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
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Serialization

let ProtocolName = "utcp"

exception private ReplyResult of Reply<obj> option

type ProtocolMessage<'T> =
  | Request of 'T
  | Response of Reply<obj>

let rec private attempt f =
  async {
    try return! f
    with :? SocketException as e when e.SocketErrorCode = SocketError.ConnectionReset || e.SocketErrorCode = SocketError.ConnectionAborted -> return! attempt f
        | :? EndOfStreamException -> return! attempt f
        | :? IOException as e -> match e.InnerException with
                                 | :? SocketException as e' when e'.SocketErrorCode = SocketError.ConnectionReset || e'.SocketErrorCode = SocketError.ConnectionAborted -> return! attempt f
                                 | _ -> return raise e
  }

module ProtocolMessage =
  let box (protocolMessage: ProtocolMessage<'T>): ProtocolMessage<obj> =
    match protocolMessage with
    | Request r -> Request(box r)
    | Response r -> Response r

  let unbox (protocolMessage: ProtocolMessage<obj>): ProtocolMessage<'T> =
    match protocolMessage with
    | Request r -> Request(unbox r)
    | Response r -> Response r

type ProtocolMessageStream(tracePrefix: string, protocolStream: ProtocolStream) =
  new (protocolStream: ProtocolStream) = new ProtocolMessageStream(String.Empty, protocolStream)

  member __.ProtocolStream = protocolStream

  member __.AsyncWriteProtocolMessage(msgId: MsgId, actorId: TcpActorId, protocolMessage: ProtocolMessage<'T>, ?serializationContext: MessageSerializationContext): Async<unit> =
    //Debug.writelfc "ProtocolMessageStream" "%s - WRITING (%A, %A, %A)" tracePrefix msgId actorId protocolMessage
    async {
      let ctx = serializationContext |> Option.map (fun mc -> mc.GetStreamingContext())
      let serializedProtocolMessage = defaultSerializer.Serialize<ProtocolMessage<'T>>(protocolMessage, ?context = ctx)
      let protocolRequest = msgId, actorId, serializedProtocolMessage : ProtocolRequest
      return! protocolStream.AsyncWriteRequest(protocolRequest)
    }

  member __.Dispose() =
    //Debug.writelfc "ProtocolMessageStream" "%s - DISPOSING" tracePrefix
    protocolStream.Dispose()

  override __.GetHashCode() = protocolStream.GetHashCode()
  override __.Equals(other: obj) =
    match other with
    | :? ProtocolMessageStream as otherStream -> protocolStream.Equals(otherStream.ProtocolStream)
    | _ -> false

  interface IDisposable with
    override self.Dispose() = self.Dispose()
        

[<AbstractClass>][<Serializable>]
type ReplyChannel =
  val mutable private timeout: int
  val private msgId: MsgId
  val private actorId: TcpActorId

  new (actorId: TcpActorId, msgId: MsgId) =
    {
      timeout = 30000
      actorId = actorId
      msgId = msgId
    }

  internal new (info: SerializationInfo, context: StreamingContext) =
    {
      timeout = info.GetInt32("timeout")
      actorId = info.GetValue("actorId", typeof<TcpActorId>) :?> TcpActorId
      msgId = info.GetValue("msgId", typeof<MsgId>) :?> MsgId
    }
  
  member r.ActorId = r.actorId
  member r.MessageId = r.msgId
  member r.Timeout with get() = r.timeout and set(timeout': int) = r.timeout <- timeout'
            
  member r.GetObjectData(info: SerializationInfo, context: StreamingContext) =
    info.AddValue("timeout", r.timeout)
    info.AddValue("actorId", r.actorId)
    info.AddValue("msgId", r.msgId)

  interface ISerializable with
    override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
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

  interface IReplyChannel with
    member __.Protocol = ProtocolName
    member self.Timeout with get() = self.Timeout and set(timeout': int) = self.Timeout <- timeout'    
    member self.ReplyUntyped(reply: Reply<obj>) =
      (self :> IReplyChannel<'T>).Reply(match reply with Value v -> Value(v :?> 'T) | Exception e -> Exception(e) : Reply<'T>)

  interface IReplyChannel<'T> with
    member self.WithTimeout(timeout: int) = self.Timeout <- timeout; self :> IReplyChannel<'T>
      
    member self.Reply(reply: Reply<'T>) = 
      //let debug x = Debug.writelfc (sprintf "utcp.ReplyChannel::%s::" (ch.MessageId.ToString())) x
      let replyLoop =
        async {
          let! replyEndPoints = self.ReplyAddress.ToEndPointsAsync()
          let replyEndPoint = List.head replyEndPoints

          //debug "REPLY CONNECTING"
          use! connection = TcpConnectionPool.AsyncAcquireConnection(self.MessageId.ToString(), replyEndPoint)

          use protocolMessageStream = new ProtocolMessageStream(new ProtocolStream(self.MessageId, connection.GetStream()))

          //write response to client
          do! protocolMessageStream.AsyncWriteProtocolMessage<obj>(self.MessageId, self.ActorId, Response(Reply.box reply))

          //debug "REPLY SENT"
          return! protocolMessageStream.ProtocolStream.AsyncReadResponse()
        }

      async {
        try
          let! protocolResponse = attempt replyLoop
          match protocolResponse with
          | Acknowledge _ ->
            //debug "REPLY ACKNOWLEDGED"
            () //reply sent ok
          | UnknownRecipient(msgId, actorId) -> 
            //debug "REPLY UNKNOWN RECIPIENT"
            raise <| new UnknownRecipientException("Remote host could not find recipient.")
          | Failure(msgId, e) -> 
            //debug "REPLY FAILURE: %A" e
            raise e
        with ex -> raise <| new CommunicationException("Unable to send reply to sender.", ex)
      } |> Async.RunSynchronously

  interface ISerializable with
    member self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
      info.AddValue("replyAddress", self.replyAddress)
      base.GetObjectData(info, context)


type ProtocolClient<'T>(actorId: TcpActorId) =
  let address = actorId.Address

  let postOnEndpoint endPoint msg =
    async {
      return ()
    }

  let postMessage msg =
    async {
      //this is memoized
      let! endPoints = address.ToEndPointsAsync()

      //fold on endpoints

      return ()
    }
  
  interface IProtocolClient<'T> with
    override __.ProtocolName = ProtocolName
    override __.ActorId = actorId :> ActorId

[<Serializable>]
type UTcp =
  inherit TcpProtocolConfiguration 

  new (publishMode: PublishMode, ?serializerName: string) = {
    inherit TcpProtocolConfiguration(publishMode, ?serializerName = serializerName)
  }
            
            //server side constructor
            //auto-select address; the addess selected will be detrmined by TcpListenerPool.Hostname
            new () = new UTcp(Publish.all)
            //server side constructor
            //specific port; the address will be detrmined by TcpListenerPool.Hostname
            new (port: int, ?serializerName: string) = 
                new UTcp(Publish.endPoints [IPEndPoint.anyIp port], ?serializerName = serializerName)

            internal new (info: SerializationInfo, context: StreamingContext) = {
                inherit TcpProtocolConfiguration(info, context)
            }

            override tcp.ProtocolName = ProtocolName
            override tcp.ConstructProtocols<'T>(actorUUId: ActorUUID, actorName: string, publishMode: PublishMode, actorRef: ActorRef<'T> option, serializerName: string option) =
                match publishMode with
                | Client addresses ->
                    if List.isEmpty addresses then raise <| new TcpProtocolConfigurationException("No addresses specified for utcp protocol.")
                    addresses |> List.map (fun addr ->
                        match ServerSideProtocolPool.TryGet(new TcpActorId(actorUUId, actorName, ProtocolName, addr)) with
                        | Some protocol when protocol.MessageType = typeof<'T> -> protocol :?> IActorProtocol<'T>
                        | _ -> new Protocol<'T>(actorUUId, actorName, ClientAddress addr, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T>
                    ) |> List.toArray
                | Server [] ->
                    let listeners = TcpListenerPool.GetListeners(IPEndPoint.any, ?serializer = serializerName)
                    if Seq.isEmpty listeners then raise <| new TcpProtocolConfigurationException("No available listeners found.")
                    [| for listener in listeners ->
                            new Protocol<'T>(actorUUId, actorName, ServerEndPoint listener.LocalEndPoint, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T> |]
                | Server ipEndPoints ->
                    [| for ipEndPoint in ipEndPoints ->
                            new Protocol<'T>(actorUUId, actorName, ServerEndPoint ipEndPoint, actorRef, ?serializerName = serializerName) :> IActorProtocol<'T> |]


        and internal ServerSideProtocolPool() =
            static let pool = Atom.atom Map.empty<TcpActorId, IActorProtocol>

            static member Register(actorId: TcpActorId, protocol: IActorProtocol) =
                Atom.swap pool (Map.add actorId protocol)

            static member UnRegister(actorId: TcpActorId) = Atom.swap pool (Map.remove actorId)

            static member TryGet(actorId: TcpActorId): IActorProtocol option = pool.Value.TryFind actorId
        
        and internal ListenerRegistrationResource(clientOnly: bool, actorId: ActorId, listener: TcpProtocolListener, recepientProcessor: Actor<RecepientProcessor>, processorF: RecepientProcessor -> Async<unit>, onDisposeF: unit -> unit) =
            static let counter = Nessos.Thespian.Agents.Agent.start Map.empty<ActorId, int>

            let start () =
                if clientOnly then
                    recepientProcessor.Start()
                    listener.RegisterRecepient(actorId, !recepientProcessor, processorF)

            let stop () =
                if clientOnly then
                    listener.UnregisterRecepient(actorId)
                    recepientProcessor.Stop()

            let inc counterMap = 
                match Map.tryFind actorId counterMap with 
                | Some count -> 
                    Map.add actorId (count + 1) counterMap
                | None -> start(); Map.add actorId 1 counterMap

            let dec counterMap =
                match Map.tryFind actorId counterMap with
                | Some 1 -> stop(); Map.remove actorId counterMap
                | Some count -> Map.add actorId (count - 1) counterMap
                | None -> counterMap

            do if clientOnly then Nessos.Thespian.Agents.Agent.sendSafe inc counter

            interface IDisposable with
                member d.Dispose() = 
                    onDisposeF()
                    if clientOnly then Nessos.Thespian.Agents.Agent.sendSafe dec counter                    

        //if actorRef is Some then the protocol instance is used in server+client mode
        //if None it is in client only mode
        and Protocol<'T> internal (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, actorRef: ActorRef<'T> option, ?serializerName: string) =
            let serializerName = serializerNameDefaultArg serializerName
            let serializer = 
                if SerializerRegistry.IsRegistered serializerName then 
                    SerializerRegistry.Resolve serializerName
                else invalidArg "Unknown serializer." "serializerName"


            let clientOnly = Option.isNone actorRef

            //we always need a listener
            //in client mode, this is allocated automatically
            //in server/client mode, this may be allocated automatically or via specified address
            let listener =
                match addressKind with
                | ServerEndPoint endPoint -> TcpListenerPool.GetListener(endPoint, serializer = serializerName)
                | ClientAddress address -> TcpListenerPool.GetListener(serializer = serializerName)

            let listenerAddress = new Address(TcpListenerPool.DefaultHostname, listener.LocalEndPoint.Port)

            //let debug i x = Debug.writelfc (sprintf "utcp(%A)::%A" listenerAddress i) x
            
            let mutable listenerLogSubcription: IDisposable option = None

            let address = 
                match addressKind with
                | ClientAddress targetAddress -> targetAddress
                | ServerEndPoint endPoint -> new Address(TcpListenerPool.DefaultHostname, listener.LocalEndPoint.Port)

            let actorId = TcpActorId(actorUUID, actorName, ProtocolName, address)

            let logEvent = new Event<Log>()

            let protocolPing targetEndPoint = 
                let debug x = Debug.writelfc (sprintf "utcp::ProtocolPing-%A" targetEndPoint) x
                let ping = async {
                    debug "CONENCTING"
                    use! connection = TcpConnectionPool.AsyncAcquireConnection("ProtocolPing", targetEndPoint)
                    
                    use protocolStream = new ProtocolStream(MsgId.Empty, connection.GetStream())
                    
                    do! protocolStream.AsyncWriteRequest(MsgId.Empty, actorId, Array.empty)
                    debug "PINGED"

                    let! response = protocolStream.AsyncReadResponse()
                    match response with
                    | Acknowledge msgId when msgId = MsgId.Empty -> debug "PING ACKNOWLEDGED"
                    | _ -> ()
                }
                async {
                    try
                        return! attempt ping
                    with e ->
                        //debug "PING FAILURE: %A" e
                        return raise (new CommunicationException("A communication error occurred.", e))
                }


            //This is a map for response message handling
            //There may be many client instances for the same actor
            //When posting with reply each registers a handler here per msgid
            //The handler is deregistered on response process
            static let responseHandleRegistry = Atom.atom Map.empty<MsgId, ProtocolStream -> MsgId -> obj -> Async<unit>>

            //response waiting registry
            let registeredResponseAsyncWaits = Atom.atom Map.empty<MsgId, AsyncResultCell<Reply<obj> option>>
            //unregister a wait for response
            let unregisterResponseAsyncWait msgId = Atom.swap registeredResponseAsyncWaits (fun regs -> Map.remove msgId regs)
            //cancel waiting for a response, cancelling causes the same result as timeout
            let cancelResponseWait msgId = registeredResponseAsyncWaits.Value.[msgId].RegisterResult None
            //register and get an async that waits for a response
            let registerAndWaitForResponse pingAsync msgId timeout = 
                let resultCell = new AsyncResultCell<Reply<obj> option>()

                Atom.swap registeredResponseAsyncWaits (fun regs -> Map.add msgId resultCell regs)
                let rec waitForReply attempts = async { //NOTE!!! Try finally?
                        let! result = resultCell.AsyncWaitResult(timeout) //throws ResponseAwaitCancelled
                        //debug (sprintf "MESSAGE POST::%A" msgId) "REPLY RECEIVED"
                        unregisterResponseAsyncWait msgId //always unregister in the end
                        match result with
                        | Some reply -> return reply //if this is None then the wait was cancelled
                        | None -> //timeout
                            if attempts = 0 then
                                return None
                            else
                                do! pingAsync
                                return! waitForReply (attempts - 1)
                                            
                    }
                waitForReply 3
                //op

            let serializationContext address =
                new MessageSerializationContext(serializer,
                    {
                        new IReplyChannelFactory with 
                            member f.Protocol = ProtocolName
                            member f.Create<'R>() = 
                                let newMsgId = MsgId.NewGuid()
                                new ReplyChannelProxy<'R>(
                                    new ReplyChannel<'R>(address, actorId, newMsgId, serializerName)
                                )
                    })

            let handleResponseMsg (protocolStream: ProtocolStream) msgId (responseMsg: obj) = 
                //let debug x = debug msgId x
                async {
                    match responseMsg with
                    | :? Reply<obj> as untypedReply ->
                        //find result cell and register result
                        match registeredResponseAsyncWaits.Value.TryFind msgId with
                        | Some asyncResultCell ->
                            //debug "REPLY RECEIVED"
                            //send acknowledgement to client and close the connection
                            try 
                                do! protocolStream.AsyncWriteResponse(Acknowledge msgId)
                            with e -> 
                                //debug "REPLY ACK WRITE FAILURE: %A" e
                                //writing the ack has failed
                                logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to respond with Acknowledgement.") |> box)

                            try 
                                asyncResultCell.RegisterResult(Some untypedReply)
                                //debug "REPLY REGISTERED"
                            with e ->
                                //debug "REPLY RESULT REG FAILURE: %A" e
                                //setting the result has failed
                                logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Setting the result for a reply has failed after responding with Acknowledgement.", e) |> box)
                        | None ->
                            //debug "REPLY RECEIVE UNKNOWN MSG"
                            logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Received a response for a non-registered message.") |> box)
                            try
                                do! protocolStream.AsyncWriteResponse(Failure(msgId, new System.Collections.Generic.KeyNotFoundException("Response message is of unknown message id.")))
                            with e ->
                                //debug "REPLY FAIL WRITE FAILURE: %A" e
                                //writing the failure has failed
                                logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to respond with Fail.") |> box)
                    | _ ->
                        //the responseMsg is of the wrong type, malformed message
                        //send failure and close connection
                        try
                            //debug "REPLY INVALID"
                            logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Received a response message of wrong type.") |> box)
                            do! protocolStream.AsyncWriteResponse(Failure(msgId, new InvalidCastException("Response message payload is of wrong type.")))
                        with e ->
                            //debug "REPLY FAIL WRITE FAILURE: %A" e
                            //writing the failure has failed
                            logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to respond with Fail.") |> box)

                        //NOTE!!! We do not register a result to the asyncResultCell
                        //The failure has been logged, and the sender has been notified
                        //The sender can then handle this by sending correct data,
                        //or the reply channel times-out (if timeout is set)
            
                }
            //this processes the UTcp protocol messages
            //processing varies wether the protocol is used in server+client mode
            //or client only mode.
            let processProtocolMessage: MsgId -> ProtocolMessage<'T> -> ProtocolStream -> Async<unit> =
                match actorRef with
                | Some actorRef -> //protocol is configured for client/server use
                    fun msgId msg protocolStream -> async {
                        match msg with
                        | Request requestMsg ->
                            try
                                //debug msgId "FORWARD TO PRINCIPAL"
                                //forward the request message to the actor
                                actorRef <-- requestMsg

                                //send acknowledgement to client and close the connection
                                try
                                    do! protocolStream.AsyncWriteResponse(Acknowledge msgId) //this may fail if the connection fails
                                with e -> //writing the ack has failed
                                    //debug msgId "MESSAGE ACK WRITE FAILURE: %A" e
                                    logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new SocketListenerException("Failed to respond with Acknowledgement.", listener.LocalEndPoint, e) |> box)
                            with e -> //forwarding the request to the actor has failed
                                //debug msgId "FORWARD TO PRINCIPAL FAILURE: %A" e
                                logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new SocketListenerException("Failed to forward message to actor after Acknowledgement.", listener.LocalEndPoint, e) |> box)
                                try
                                    do! protocolStream.AsyncWriteResponse(Failure(msgId, e))
                                with e ->
                                    //debug msgId "MESSAGE FAIL WRITE FAILURE: %A" e
                                    logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new SocketListenerException("Failed to respond with Failure.", listener.LocalEndPoint, e) |> box)

                        | Response responseMsg -> 
                            //debug msgId "PROCESS RESPOSNE"
                            match responseHandleRegistry.Value.TryFind msgId with
                            | Some processResponseMessgage -> do! processResponseMessgage protocolStream msgId responseMsg //throws nothing
                            | None -> do! handleResponseMsg protocolStream msgId responseMsg //this throws nothing
                    }
                | None -> //protocol is configured only for client use
                    fun msgId msg protocolStream -> async {
                        match msg with
                        | Response responseMsg -> 
                            //debug msgId "PROCESS RESPOSNE"
                            match responseHandleRegistry.Value.TryFind msgId with
                            | Some processResponseMessgage -> do! processResponseMessgage protocolStream msgId responseMsg //throws nothing
                            | None -> do! handleResponseMsg protocolStream msgId responseMsg //this throws nothing
                        | Request _ ->
                            logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Received a request for a client side only protocol.") |> box)
                    }
            
            //this receives serialized messages from the listener
            //deserializes and then passes processProtocolMessage for further processing
            let recepientProcessorBehavior ((msgId, payload, protocolStream): RecepientProcessor) = async {
                //make sure connection is always properly closed after we are finished
                //use _ = protocolStream

                try
                    //debug msgId "MESSAGE PROCESS START"
                    //second stage deserialization
                    let msg = serializer.Deserialize<ProtocolMessage<obj>>(payload) |> ProtocolMessage.unbox //throws SerializationException, InvalidCastException

                    //debug msgId "MESSAGE DESERIALIZED: %A" msg

                    do! processProtocolMessage msgId msg protocolStream //throws nothing

                    //debug msgId "MESSAGE PROCESS END"
                with e ->
                    //debug msgId "MESSAGE PROCESS FAILURE: %A" e
                    //probably an exception related to deserialization
                    //send with fail
                    try
                        do! protocolStream.AsyncWriteResponse(Failure(msgId, e))
                    with e -> //this again may fail
                        //debug msgId "MESSAGE FAIL WRITE FAILURE: %A" e 
                        logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to send a Failure response.", e) |> box)
            }
            let recepientProcessor = Actor.bind <| Behavior.stateless recepientProcessorBehavior
            
            let rec postMessageOnEndPoint (targetEndPoint: IPEndPoint) (msgId: MsgId) (msg: 'T) = 
                //let debug x = debug (sprintf "MESSAGE POST::%A" msgId) x
                async {
                    //debug "POSTING MESSAGE %A" msg
                    //debug "CONNECTING"
                    try
                        use! connection = TcpConnectionPool.AsyncAcquireConnection(msgId.ToString(), targetEndPoint)

                        //do! connection.AsyncConnent targetEndPoint
                        let stream = connection.GetStream()
    //                    use protocolStream = new ProtocolStream(msgId, stream, fun () -> sprintfn "CLDISPN %A %A" connection.UnderlyingClient.Client.LocalEndPoint connection.UnderlyingClient.Client.RemoteEndPoint; connection.Return())
                        use protocolStream = new ProtocolStream(msgId, stream)
                        //serialize message with the reply patching serialization context
                        let context = serializationContext listenerAddress
                        let protocolMessage = serializer.Serialize(Request msg |> ProtocolMessage.box, context.GetStreamingContext())

                        //handle foreign reply channels provided in the context
                        let foreignReplyChannelHandlers, foreignReplyChannelsMsgids =
                            context.ReplyChannelOverrides |> List.map (fun (foreignReplyChannel, nativeReplyChannel) ->
                                //the foreign reply channel will not be posted through the message, but the native one will
                                //we need to wait for the response on the native rc and forward it to the foreign rc
                                //each native reply channel has a new msgId
                                let nativeReplyChannel = nativeReplyChannel :?> ReplyChannel
                                let awaitResponse = registerAndWaitForResponse (protocolPing targetEndPoint) nativeReplyChannel.MessageId foreignReplyChannel.Timeout
                                //if in client mode make sure the recipient processor is registered with the listener
                                let listenerRegistration = new ListenerRegistrationResource(clientOnly, actorId, listener, recepientProcessor, recepientProcessorBehavior, ignore)
                                async {
                                    use _ = listenerRegistration //make sure unregistration happens
                                    //wait for the native rc result
                                    let! result = awaitResponse
                                    match result with
                                    | Some replyResult ->    
                                        //forward to foreign rc
                                        try
                                            foreignReplyChannel.ReplyUntyped(replyResult)
                                        with e ->
                                            logEvent.Trigger(Warning, LogSource.Protocol ProtocolName, new CommunicationException("Failed to forward a reply.", e) |> box)
                                    | None -> ()
                                        //Either the wait timed-out or it was cancelled.
                                        //If the wait-timed out on our native rc, then so it will on the foreign rc.
                                        //If the response wait was cancelled then we failed to post the message.
                                        //This means that the client posting the message will be notified by an exception.
                                        //It is the client's responsibility to handle this and perform any action on the
                                        //foreign rcs. 
                                        //Thus, we do nothing in this case.
                                
                                },
                                nativeReplyChannel.MessageId
                                //we map to pairs of async reply forward computation and the native rc msgId
                                //because if something goes wrong in the message post
                                //we need to use the msgIds to unregister the waits
                            ) |> List.unzip
                        //start handling the replies for foreign reply channels
                        foreignReplyChannelHandlers |> Async.Parallel |> Async.Ignore |> Async.Start

                        //create and write protocol request write message
                        let protocolRequest = msgId, actorId, protocolMessage : ProtocolRequest

                        try
                            //debug "POSTING"
                            do! protocolStream.AsyncWriteRequest(protocolRequest)
                
                            //debug "POSTED"

                            return! protocolStream.AsyncReadResponse()
                        with e ->
                            //debug "POST WRITE FAILURE: %A" e
                            //cancel the async handling the foreign rc responses
                            foreignReplyChannelsMsgids |> List.iter cancelResponseWait
                            return! Async.Raise e
                    with e ->
                        //debug "POST FAILURE: %A" e
                        return! Async.Raise e
                }

            let postMessageAndProcessResponse endPoint msgId msg = 
                //let debug x = debug (sprintf "MESSAGE POST::%A" msgId) x
                async {
                    let! protocolResponse = attempt (postMessageOnEndPoint endPoint msgId msg)

                    return match protocolResponse with
                                 | Acknowledge _ -> 
                                    //debug "POST SUCCESS"
                                    Choice1Of2() //post was successfull
                                 | UnknownRecipient _ ->
                                     //debug "POST UNKNOWN RECIPIENT"
                                     Choice2Of2(new UnknownRecipientException("Remote host could not find the message recipient.") :> exn)
                                 | Failure(_, e) -> 
                                     //debug "POST FAILURE: %A" e
                                     Choice2Of2(new CommunicationException("Message delivery failed on the remote host.", e) :> exn)
                }

            let rec postMessageLoop endPoints msgId msg = async {
                match endPoints with
                | [endPoint] ->
                    let! r = postMessageAndProcessResponse endPoint msgId msg
                             |> Async.Catch
                    match r with
                    | Choice1Of2 r' ->
                        match r' with
                        | Choice1Of2 _ -> return ()
                        | Choice2Of2 e -> return! raisex e
                    | Choice2Of2 e -> return! raisex e
                | endPoint::rest ->
                    let! r = postMessageAndProcessResponse endPoint msgId msg
                             |> Async.Catch
                    match r with
                    | Choice1Of2 r' ->
                        match r' with
                        | Choice1Of2 _ -> return ()
                        | Choice2Of2 _ ->
                            return! postMessageLoop rest msgId msg
                    | Choice2Of2 _ ->
                        return! postMessageLoop rest msgId msg
                | _ -> return! raisex (new SystemException("utcp :: postMessageLoop :: Invalid State :: target endpoints exhausted"))
            }

            let postMessage msgId msg = async {
                let! endPoints = address.ToEndPointsAsync()
                return! postMessageLoop endPoints msgId msg
            }

            let postMessageWithReplyOnEndPoint (targetEndPoint: IPEndPoint) (msgId: MsgId) (msg: 'T) (timeout: int) = 
                //let debug x = debug (sprintf "MESSAGE POST::%A" msgId) x
                async {
                    //if in client mode make sure the recipient processor is registered with the listener
                    Atom.swap responseHandleRegistry (Map.add msgId handleResponseMsg)
                    //use _ = new ListenerRegistrationResource(clientOnly, actorId, listener, recipientProcessor, (fun () -> Atom.swap responseHandleRegistry (Map.remove msgId)))
                    use _ = if not (listener.IsRecepientRegistered actorId) then
                                listener.RegisterMessageProcessor(msgId, recepientProcessorBehavior)
                                { new IDisposable with override __.Dispose() = Atom.swap responseHandleRegistry (Map.remove msgId); listener.UnregisterMessageProcessor(msgId) }
                            else { new IDisposable with override __.Dispose() = Atom.swap responseHandleRegistry (Map.remove msgId) }

                    let awaitResponse = registerAndWaitForResponse (protocolPing targetEndPoint) msgId timeout

                    let! r = postMessageAndProcessResponse targetEndPoint msgId msg
                             |> Async.Catch
                    match r with
                    | Choice1Of2 r' ->
                        match r' with
                        | Choice1Of2 _ ->
                            //debug "WAITING FOR REPLY"
                            let! response = Async.Catch awaitResponse
                            match response with
                            | Choice1Of2 r ->
                                //debug "REPLY RETURNED"
                                return Choice1Of2 r
                            | Choice2Of2 e -> return Choice2Of2 e
                        | Choice2Of2 e ->
                            unregisterResponseAsyncWait msgId
                            return Choice2Of2 e
                    | Choice2Of2 e ->
                        unregisterResponseAsyncWait msgId
                        return Choice2Of2 e
                }

            let rec postMessageWithReplyLoop endPoints msgId msg timeout = async {
                match endPoints with
                | [endPoint] ->
                    let! r = postMessageWithReplyOnEndPoint endPoint msgId msg timeout
                    match r with
                    | Choice1Of2 reply -> return reply
                    //| Choice2Of2(CommunicationException(_, _, _, InnerException e)) -> return! raisex e
                    | Choice2Of2((CommunicationException _) as e) -> return! raisex e
                    | Choice2Of2 e -> return! raisex <| new CommunicationException("A communication error occurred.", e)
                | endPoint::rest ->
                    let! r = postMessageWithReplyOnEndPoint endPoint msgId msg timeout
                    match r with
                    | Choice1Of2 reply -> return reply
                    | Choice2Of2 _ ->
                        return! postMessageWithReplyLoop rest msgId msg timeout
                | _ -> return! raisex (new SystemException("utcp :: postMessageWithReplyLoop :: Invalid State :: target endpoints exhausted"))
            }

            let postMessageWithReply (msgId: MsgId) (msg: 'T) (timeout: int) = async {
                let! endPoints = address.ToEndPointsAsync()
                return! postMessageWithReplyLoop endPoints msgId msg timeout
            }

            //client+server use constructor
            new (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, actorRef: ActorRef<'T>, ?serializerName: string) =
                new Protocol<'T>(actorUUID, actorName, addressKind, Some actorRef, ?serializerName = serializerName)

            //client only use constructor
            new (actorUUID: ActorUUID, actorName: string, addressKind: ProtocolAddressKind, ?serializerName: string) =
                new Protocol<'T>(actorUUID, actorName, addressKind, None, ?serializerName = serializerName)

            member private __.Listener = listener

            //main post message with reply, timeout returns None
            member private p.TryPostWithReply(msgBuilder: IReplyChannel<'R> -> 'T, ?timeoutOverride: int): Async<'R option> = async {
                let msgId = MsgId.NewGuid()

                //TODO!!! Make reply channel always have a serializer name. See other todos/notes.
                let replyChannel = new ReplyChannel<'R>(listenerAddress, actorId, msgId, serializer = serializerName)

                //we always wrap the reply channel in the rc proxy so that it can be considered as a foreign rc
                //by other protocols
                let msg = msgBuilder (new ReplyChannelProxy<'R>(replyChannel))
                //override timeout set on the reply channel, if timeoutOverride is set
                timeoutOverride |> Option.iter (fun timeout -> replyChannel.Timeout <- timeout)

                let! result = postMessageWithReply msgId msg replyChannel.Timeout
                return result |> Option.map Reply.unbox<'R>
                              |> Option.map (function Value v -> v | Exception e -> raise (new MessageHandlingException("Remote Actor threw exception while handling message.", actorName, actorUUID, e)))
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
                    new UTcp(ProtocolAddressKind.toPublishMode addressKind, serializerName) :> IProtocolConfiguration |> Some

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

                member p.PostAsync(msg: 'T) = async {
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
                        listenerLogSubcription <- listener.Log //|> Observable.choose (function Warning, source, (:? SocketResponseException as e) when e.ActorId = actorId -> Some(Warning, source, e :> obj) | _ -> None)
                                                  |> Observable.subscribe logEvent.Trigger
                                                  |> Some

                        recepientProcessor.Start()
                        listener.RegisterRecepient(actorId, !recepientProcessor, recepientProcessorBehavior)
                        ServerSideProtocolPool.Register(actorId, p)
                    | _ -> ()

                member p.Stop() =
                    match listenerLogSubcription with
                    | Some disposable ->
                        ServerSideProtocolPool.UnRegister(actorId)
                        listener.UnregisterRecepient(actorId)
                        recepientProcessor.Stop()
                        disposable.Dispose()
                        listenerLogSubcription <- None
                    | None -> ()
