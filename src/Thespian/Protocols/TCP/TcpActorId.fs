namespace Nessos.Thespian.Remote.TcpProtocol

open System
open System.IO

open Nessos.Thespian
open Nessos.Thespian.Utils

[<Serializable>]
type TcpActorId(actorName : string, protocolName : string, address : Address) = 
    inherit ActorId(actorName)
    let toString = protocolName + "/" + (address.ToString()) + "/" + actorName
    member __.ProtocolName = protocolName
    member __.Address = address
    
    override self.CompareTo(otherActorId : ActorId) : int = 
        match otherActorId with
        | :? TcpActorId as otherId -> 
            compareOn (fun (aid : TcpActorId) -> (aid.ProtocolName + aid.Name), aid.Address) self otherId
        | _ -> self.ToString().CompareTo(otherActorId.ToString())
    
    override __.ToString() = toString
    
    member __.BinarySerialize(writer : BinaryWriter) = 
        //actorName: string
        writer.Write actorName
        //protocolName: string
        writer.Write protocolName
        //address: Address
        //custom binary serialization
        address.BinarySerialize(writer)
    
    static member BinaryDeserialize(reader : BinaryReader) : TcpActorId = 
        //actorName: string
        let actorName = reader.ReadString()
        //protocolName: string
        let protocolName = reader.ReadString()
        //address: Address
        //custom binary serialization
        let address = Address.BinaryDeserialize(reader)
        new TcpActorId(actorName, protocolName, address)
