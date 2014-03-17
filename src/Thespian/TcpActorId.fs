namespace Nessos.Thespian.Remote.TcpProtocol

    open System
    open System.IO
    open System.Net
    open System.Net.Sockets
    open System.Threading
    open System.Runtime.Serialization

    open Nessos.Thespian
    open Nessos.Thespian.Utils
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote
    open Nessos.Thespian.Remote.SocketExtensions
    open Nessos.Thespian.DisposableExtensions

    [<Serializable>]
    type TcpActorId(uuid: ActorUUID, actorName: string, protocolName: string, address: Address) =
        inherit ActorId()

        let uuidPart, namePart = TcpActorId.GetUUIDAndNameParts(uuid, actorName)

        member actorId.ProtocolName = protocolName
        member actorId.Address = address
        member actorId.UUID = uuidPart
        member actorId.Name = namePart
        
        override actorId.CompareTo(otherActorId: ActorId): int =
//            actorId.ToString().CompareTo(otherActorId.ToString())
            match otherActorId with
            | :? TcpActorId as otherId ->
                compareOn (fun (aid: TcpActorId) -> (aid.ProtocolName + aid.UUID + aid.Name), aid.Address) actorId otherId
            | _ -> 
                actorId.ToString().CompareTo(otherActorId.ToString())

        override actorId.ToString() = sprintf "%s/%O/%s/%s" protocolName address uuidPart namePart

        member __.BinarySerialize(writer: BinaryWriter) =
            //actorUUID: GUID
            writer.Write(uuid.ToByteArray())
            //actorName: string
            writer.Write actorName
            //protocolName: string
            writer.Write protocolName
            //address: Address
            //custom binary serialization
            address.BinarySerialize(writer)

        static member BinaryDeserialize(reader: BinaryReader): TcpActorId =
            //actorUUID: GUID (16 bytes)
            let actorUUIDBinary = reader.ReadBytes(16)
            let actorUUID = new ActorUUID(actorUUIDBinary)
            //actorName: string
            let actorName = reader.ReadString()
            //protocolName: string
            let protocolName = reader.ReadString()
            //address: Address
            //custom binary serialization
            let address = Address.BinaryDeserialize(reader)
            new TcpActorId(actorUUID, actorName, protocolName, address)

        static member GetUUIDAndNameParts(actorUUID: ActorUUID, actorName: string) = 
            if actorUUID = ActorUUID.Empty && actorName = String.Empty then "*", "*"
            else if actorName = String.Empty then actorUUID.ToString(), "*"
            else "*", actorName
        static member TryUUIDPartToActorUUID (uuidPart: string): ActorUUID option =
            if uuidPart = "*" then Some ActorUUID.Empty
            else match ActorUUID.TryParse(uuidPart) with
                 | (true, uuid) -> Some uuid
                 | _ -> None
        static member UuidPartToActorUUID (uuidPart: string): ActorUUID = 
            TcpActorId.TryUUIDPartToActorUUID uuidPart |> Option.get
        static member NamePartToActorName (namePart: string): string =
            if namePart = "*" then String.Empty
            else namePart

