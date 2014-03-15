namespace Thespian.Remote.TcpProtocol

    open System
    open System.IO
    open System.Net
    open System.Net.Sockets
    open System.Threading
    open System.Runtime.Serialization

    open Thespian
    open Thespian.Utils
    open Thespian.Serialization
    open Thespian.DisposableExtensions
    open Thespian.Remote
    open Thespian.Remote.SocketExtensions
    open Thespian.Remote.TcpProtocol.BinarySerializationExtensions

    type MsgId = Guid

    //A message received by SocketProtocolListener
    //Each has a message id, the id of the recipient actor and a binary payload
    //The listener forwards the payload to the particular protocol object associated
    //with the actor / actorRef
    type ProtocolRequest = MsgId * TcpActorId * byte[]
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
        with
            static member BinaryDeserialize(reader: BinaryReader) =
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
                    let serializer = new BinaryFormatterMessageSerializer(false) :> IMessageSerializer
                    let e = serializer.Deserialize(obj(), eBinary)
                    Failure(msgId, unbox e)
                | _ -> //invalid
                    raise <| new SerializationException("TcpProtocol: Invalid ProtocolResponse binary format.")

            member protocolResponse.BinarySerialize(writer: BinaryWriter) =
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
                    let serializer = new BinaryFormatterMessageSerializer(false) :> IMessageSerializer
                    let eBinary = serializer.Serialize(obj(), e)
                    writer.WriteByteArray(eBinary)

    module Message = 

        let readMessage (stream: Stream) (deserializeF: BinaryReader -> 'T): Async<'T> = async {
            //read total length
            let! messageLengthBinary = stream.AsyncRead(4)
//            let messageLengthBinary = Array.zeroCreate 4 : byte[]
//            let bytesRead = stream.Read(messageLengthBinary, 0, 4)

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
            return deserializeF reader
        }

        let writeMessage (stream: Stream) (serializeF: 'T -> byte[]) (message: 'T) = async {
            let payloadBinary = serializeF message

            use writer = new BinaryWriter(new MemoryStream())
            writer.Write(payloadBinary.Length)
            writer.Write(payloadBinary)

            let memoryStream = writer.BaseStream :?> MemoryStream
            let messageBinary = memoryStream.ToArray()

            do! stream.AsyncWrite messageBinary
        }

        module ProtocolRequest = 
            let serialize ((msgId, actorId, payload): ProtocolRequest) =
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


            let deserialize (reader: BinaryReader) =
                //msgId: GUID (16 bytes)
                let msgIdBinary = reader.ReadBytes(16)
                let msgId = new MsgId(msgIdBinary)
                //actorId: ActorId
                //custom binary serialization
                let actorId = TcpActorId.BinaryDeserialize(reader)
                //payload: byte[]
                let payload = reader.ReadByteArray()
            
                (msgId, actorId, payload): ProtocolRequest

            let read (stream: Stream): Async<ProtocolRequest> = 
                readMessage stream deserialize

            let write (stream: Stream) (protocolRequest: ProtocolRequest): Async<unit> =
                writeMessage stream serialize protocolRequest

        module ProtocolResponse =
            let serialize (protocolResponse: ProtocolResponse) =
                use memoryStream = new MemoryStream()
                use writer = new BinaryWriter(memoryStream)

                protocolResponse.BinarySerialize(writer)

                memoryStream.ToArray()

            let deserialize (reader: BinaryReader) = ProtocolResponse.BinaryDeserialize(reader)

            let read (stream: Stream): Async<ProtocolResponse> =
                readMessage stream deserialize

            let write (stream: Stream) (protocolResponse: ProtocolResponse): Async<unit> =
                writeMessage stream serialize protocolResponse
            

//    type ProtocolStream(msgId: MsgId, stream: Stream, disposeF: unit -> unit) =
    type ProtocolStream(msgId: MsgId, stream: Stream) =
        //let debug x = Debug.writelfc (sprintf "ProtocolStream::%s::" (msgId.ToString())) x

        let mutable retain = true

//        new (msgId: MsgId, stream: Stream) = new ProtocolStream(msgId, stream, ignore)

        static member private MsgId(ps: ProtocolStream) = ps.Id

//        static member InitReadAsync(stream: Stream, disposeF: unit -> unit) = async {
        static member InitReadAsync(stream: Stream) = async {

            let! ((msgId, _, _) as protocolRequest) = Message.ProtocolRequest.read stream

//            return protocolRequest, new ProtocolStream(msgId, stream, disposeF)
            return protocolRequest, new ProtocolStream(msgId, stream)
        }

//        static member InitReadAsync(stream: Stream) = ProtocolStream.InitReadAsync(stream, ignore)

        member private __.Id = msgId
        
        member __.AsyncWriteRequest(protocolRequest: ProtocolRequest): Async<unit> = async {
            //debug "WRITE REQUEST %A" protocolRequest
            return! Message.ProtocolRequest.write stream protocolRequest
        }

        member __.AsyncReadRequest(): Async<ProtocolRequest> = async {
            let! r = Message.ProtocolRequest.read stream
            //debug "READ REQUEST %A" r
            return r
        }
        
        member __.AsyncWriteResponse(protocolResponse: ProtocolResponse): Async<unit> = async {
            //debug "WRITE RESPOSNE %A" protocolResponse
            return! Message.ProtocolResponse.write stream protocolResponse
        }

        member __.AsyncReadResponse(): Async<ProtocolResponse> = async {
            let! r = Message.ProtocolResponse.read stream
            //debug "READ RESPONSE %A" r
            return r
        }

        member __.Retain with get() = retain
                              and set(value: bool) = retain <- value

        member __.Dispose() = 
            //debug "DISPOSE"
            stream.Dispose()
            //disposeF()

        override __.GetHashCode() = hash msgId
        override s.Equals(other: obj) = equalsOn ProtocolStream.MsgId s other

        interface IComparable with
            member s.CompareTo(other: obj) = 
                compareOn ProtocolStream.MsgId s other

        interface IDisposable with
            member s.Dispose() = s.Dispose()
        
            