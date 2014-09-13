namespace Nessos.Thespian.Remote.TcpProtocol

open System
open System.IO
open System.Net.Sockets
open System.Threading
open System.Runtime.Serialization

open Nessos.Thespian
open Nessos.Thespian.Utilities

type MsgId = Guid

//A message received by SocketProtocolListener
//Each has a message id, the id of the recipient actor and a binary payload
//The listener forwards the payload to the particular protocol object associated
//with the actor / actorRef
type ProtocolRequest = MsgId * TcpActorId * byte []

//The messages sent by the listener to the client
type ProtocolResponse = 
    //the message has been successfully received, forwarded to the actor's protocol, deserialization succeeded and it is ready to be processed by the receiving actor/actorRef
    | Acknowledge of MsgId
    //the listener was unable to find the receiving actor/actorRef
    | UnknownRecipient of MsgId * TcpActorId
    //message has been received, forwarded to the actor's protocol but something went wrong during further processing; probably either invalid cast
    //(client - server type mismatch) or a deserialization exception
    //The exception that occurs is carried in the message.
    | Failure of MsgId * exn
    
    static member BinaryDeserialize(reader : BinaryReader) = 
        //union case tag
        let case = reader.ReadInt16()
        match case with
        | 1s -> //Ackgnowledge
                
            //msgId: Guid (16 bytes)
            let msgIdBinary = reader.ReadBytes(16)
            let msgId = new MsgId(msgIdBinary)
            Acknowledge msgId
        | 2s -> //UnknownRecipient
                
            //msgId: Guid (16 bytes)
            let msgIdBinary = reader.ReadBytes(16)
            let msgId = new MsgId(msgIdBinary)
            //actorId: TcpActorId
            //custom serialization
            let actorId = TcpActorId.BinaryDeserialize(reader)
            UnknownRecipient(msgId, actorId)
        | 3s -> //Failure
                
            //msgId: Guid (16 bytes)
            let msgIdBinary = reader.ReadBytes(16)
            let msgId = new MsgId(msgIdBinary)
            //e: exn
            //binary formatter serialization
            //byte array serialization
            let eBinary = reader.ReadByteArray()
            let serializer = Serialization.defaultSerializer
            let e = serializer.Deserialize<exn> eBinary
            Failure(msgId, unbox e)
        | _ -> //invalid
               
            raise <| new SerializationException("TcpProtocol: Invalid ProtocolResponse binary format.")
    
    member protocolResponse.BinarySerialize(writer : BinaryWriter) = 
        match protocolResponse with
        | Acknowledge msgId -> 
            //union case tag: 1
            writer.Write 1s
            //msgId: Guid (16 bytes)
            writer.Write(msgId.ToByteArray())
        | UnknownRecipient(msgId, actorId) -> 
            //union case tag: 2
            writer.Write 2s
            //msgId: Guid (16 bytes)
            writer.Write(msgId.ToByteArray())
            //actorId: TcpActorId
            //custom serialization
            actorId.BinarySerialize(writer)
        | Failure(msgId, e) -> 
            //union case tag: 3
            writer.Write 3s
            //msgId: Guid (16 bytes)
            writer.Write(msgId.ToByteArray())
            //e: exn
            //binary formatter serialization
            //byte array serialization
            let serializer = Serialization.defaultSerializer
            let eBinary = serializer.Serialize<exn>(e)
            writer.WriteByteArray(eBinary)

type ProtocolNetworkStream(tcpClient : TcpClient, ?keepOpen : bool) = 
    inherit NetworkStream(tcpClient.Client, not <| defaultArg keepOpen true)
    let keepOpen = defaultArg keepOpen true
    let socket = tcpClient.Client
    abstract FaultDispose : unit -> unit
    
    override self.FaultDispose() = 
        self.Dispose()
        if keepOpen then 
            if tcpClient.Client <> null then tcpClient.Client.LingerState = new LingerOption(true, 0)
                                             |> ignore
        tcpClient.Close()
    
    //returns disposable that releases the socket even if keepOpen = true
    member self.Acquire() = 
        { new IDisposable with
              member __.Dispose() = 
                  self.Dispose()
                  tcpClient.Close() }
    
    member self.TryAsyncRead(buffer : byte [], offset : int, size : int, timeout : int) : Async<int option> = 
        if timeout = 0 then async.Return None
        elif timeout = Timeout.Infinite then async { let! r = self.AsyncRead(buffer, offset, size)
                                                     return Some r }
        else Async.TryFromBeginEnd(buffer, offset, size, self.BeginRead, self.EndRead, timeout, self.FaultDispose)
    
    member self.TryAsyncRead(count : int, timeout : int) : Async<byte [] option> = 
        async { 
            let buffer = Array.zeroCreate count
            let i = ref 0
            while i.Value >= 0 && i.Value < count do
                let! n = self.TryAsyncRead(buffer, i.Value, count - i.Value, timeout)
                match n with
                | Some i' -> 
                    i := i.Value + i'
                    if i' = 0 then 
                        return! Async.Raise 
                                <| new EndOfStreamException("Reached end of stream before reading all requested data.")
                | None -> i := -1
            if i.Value = -1 then return None
            else return Some buffer
        }
    
    member self.TryAsyncWrite(buffer : byte [], timeout : int, ?offset : int, ?count : int) : Async<unit option> = 
        let offset = defaultArg offset 0
        let count = defaultArg count buffer.Length
        if timeout = 0 then async.Return None
        elif timeout = Timeout.Infinite then async { let! _ = self.AsyncWrite(buffer, offset, count)
                                                     return Some() }
        else Async.TryFromBeginEnd(buffer, offset, count, self.BeginWrite, self.EndWrite, timeout, self.FaultDispose)

module Message = 
    let mutable DefaultReadTimeout = 30000
    let mutable DefaultWriteTimeout = 30000
    
    let tryReadMessage (stream : ProtocolNetworkStream) (deserializeF : BinaryReader -> 'T) (timeout : int) : Async<'T option> = 
        async { 
            //read total length
            let! r = stream.TryAsyncRead(4, timeout)
            match r with
            | Some messageLengthBinary -> 
                //setup reader and writer
                //writer is used to write byte arrays read from
                //the network stream
                //reader is used to deserialize
                use writer = new BinaryWriter(new MemoryStream())
                use reader = new BinaryReader(writer.BaseStream)
                writer.Write(messageLengthBinary)
                writer.BaseStream.Position <- 0L
                //get the message length int
                let messageLength = reader.ReadInt32()
                //read the message
                let! messageBinary = stream.AsyncRead(messageLength)
                //and copy to memory stream
                writer.Write(messageBinary)
                writer.BaseStream.Position <- 4L
                //finally deserialize
                return Some(deserializeF reader)
            | None -> return None
        }
    
    let readMessage (stream : ProtocolNetworkStream) (deserializeF : BinaryReader -> 'T) : Async<'T> = 
        async { 
            let! r = tryReadMessage stream deserializeF Timeout.Infinite
            match r with
            | Some r -> return r
            | None -> return! Async.Raise(new TimeoutException()) //dead code
        }
    
    let tryWriteMessage (stream : ProtocolNetworkStream) (serializeF : 'T -> byte []) (message : 'T) (timeout : int) : Async<unit option> = 
        async { 
            let payloadBinary = serializeF message
            use writer = new BinaryWriter(new MemoryStream())
            writer.Write(payloadBinary.Length)
            writer.Write(payloadBinary)
            let memoryStream = writer.BaseStream :?> MemoryStream
            let messageBinary = memoryStream.ToArray()
            return! stream.TryAsyncWrite(messageBinary, timeout)
        }
    
    let writeMessage (stream : ProtocolNetworkStream) (serializeF : 'T -> byte []) (message : 'T) = 
        async { 
            let! r = tryWriteMessage stream serializeF message Timeout.Infinite
            match r with
            | Some() -> return ()
            | None -> return! Async.Raise(new TimeoutException()) //dead code
        }
    
    module ProtocolRequest = 
        let serialize ((msgId, actorId, payload) : ProtocolRequest) = 
            use memoryStream = new MemoryStream()
            use writer = new BinaryWriter(memoryStream)
            //msgId: GUID (16 bytes)
            writer.Write(msgId.ToByteArray())
            //actorId: ActorId
            //custom binary serialization
            actorId.BinarySerialize(writer)
            //payload: byte[]
            writer.WriteByteArray(payload)
            memoryStream.ToArray()
        
        let deserialize (reader : BinaryReader) = 
            //msgId: GUID (16 bytes)
            let msgIdBinary = reader.ReadBytes(16)
            let msgId = new MsgId(msgIdBinary)
            //actorId: ActorId
            //custom binary serialization
            let actorId = TcpActorId.BinaryDeserialize(reader)
            //payload: byte[]
            let payload = reader.ReadByteArray()
            (msgId, actorId, payload) : ProtocolRequest
        
        let tryRead (stream : ProtocolNetworkStream) (timeout : int) : Async<ProtocolRequest option> = 
            tryReadMessage stream deserialize timeout
        let read (stream : ProtocolNetworkStream) : Async<ProtocolRequest> = readMessage stream deserialize
        let tryWrite (stream : ProtocolNetworkStream) (protocolRequest : ProtocolRequest) (timeout : int) : Async<unit option> = 
            tryWriteMessage stream serialize protocolRequest timeout
        let write (stream : ProtocolNetworkStream) (protocolRequest : ProtocolRequest) : Async<unit> = 
            writeMessage stream serialize protocolRequest
    
    module ProtocolResponse = 
        let serialize (protocolResponse : ProtocolResponse) = 
            use memoryStream = new MemoryStream()
            use writer = new BinaryWriter(memoryStream)
            protocolResponse.BinarySerialize(writer)
            memoryStream.ToArray()
        
        let deserialize (reader : BinaryReader) = ProtocolResponse.BinaryDeserialize(reader)
        let tryRead (stream : ProtocolNetworkStream) (timeout : int) : Async<ProtocolResponse option> = 
            tryReadMessage stream deserialize timeout
        let read (stream : ProtocolNetworkStream) : Async<ProtocolResponse> = readMessage stream deserialize
        let tryWrite (stream : ProtocolNetworkStream) (protocolResponse : ProtocolResponse) (timeout : int) : Async<unit option> = 
            tryWriteMessage stream serialize protocolResponse timeout
        let write (stream : ProtocolNetworkStream) (protocolResponse : ProtocolResponse) : Async<unit> = 
            writeMessage stream serialize protocolResponse

type ProtocolStream(msgId : MsgId, stream : ProtocolNetworkStream, ?readTimeout : int, ?writeTimeout : int) = 
    let readTimeout = defaultArg readTimeout Message.DefaultReadTimeout
    let writeTimeout = defaultArg writeTimeout Message.DefaultWriteTimeout
    let mutable retain = true
    static let getMsgId (s : ProtocolStream) = s.Id
    
    static member AsyncCreateRead(tcpClient : TcpClient, readTimeout : int, writeTimeout : int, ?keepOpen : bool) : Async<(ProtocolRequest * ProtocolStream) option> = 
        async { 
            let stream = new ProtocolNetworkStream(tcpClient, ?keepOpen = keepOpen)
            let! r = Message.ProtocolRequest.tryRead stream readTimeout
            match r with
            | Some((msgId, _, _) as protocolRequest) -> 
                return Some(protocolRequest, new ProtocolStream(msgId, stream, readTimeout, writeTimeout))
            | None -> return None
        }
    
    member __.Id = msgId
    member __.NetworkStream = stream
    member __.AsyncReadRequest() : Async<ProtocolRequest> = Message.ProtocolRequest.read stream
    member __.AsyncWriteRequest(protocolRequest : ProtocolRequest) : Async<unit> = 
        Message.ProtocolRequest.write stream protocolRequest
    member __.AsyncReadResponse() : Async<ProtocolResponse> = Message.ProtocolResponse.read stream
    member __.AsyncWriteResponse(protocolResponse : ProtocolResponse) : Async<unit> = 
        Message.ProtocolResponse.write stream protocolResponse
    member __.TryAsyncReadRequest() : Async<ProtocolRequest option> = Message.ProtocolRequest.tryRead stream readTimeout
    member __.TryAsyncWriteRequest(protocolRequest : ProtocolRequest) : Async<unit option> = 
        Message.ProtocolRequest.tryWrite stream protocolRequest writeTimeout
    member __.TryAsyncReadResponse() : Async<ProtocolResponse option> = 
        Message.ProtocolResponse.tryRead stream readTimeout
    member __.TryAsyncWriteResponse(protocolResponse : ProtocolResponse) : Async<unit option> = 
        Message.ProtocolResponse.tryWrite stream protocolResponse writeTimeout
    
    member __.Retain 
        with get () = retain
        and set (value : bool) = retain <- value
    
    member __.Dispose() = stream.Dispose()
    member __.Acquire() = stream.Acquire()
    override __.GetHashCode() = hash msgId
    override self.Equals(other : obj) = equalsOn getMsgId self other
    
    interface IComparable with
        member self.CompareTo(other : obj) = compareOn getMsgId self other
    
    interface IDisposable with
        member self.Dispose() = self.Dispose()
