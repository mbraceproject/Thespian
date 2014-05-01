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
type TcpActorId(actorName: string, protocolName: string, address: Address) =
  inherit ActorId(actorName)

  member __.ProtocolName = protocolName
  member __.Address = address
        
  override actorId.CompareTo(otherActorId: ActorId): int =
    match otherActorId with
    | :? TcpActorId as otherId ->
      compareOn (fun (aid: TcpActorId) -> (aid.ProtocolName + aid.Name), aid.Address) actorId otherId
    | _ -> actorId.ToString().CompareTo(otherActorId.ToString())

  override actorId.ToString() = sprintf "%s/%O/%s" protocolName address actorName

  member __.BinarySerialize(writer: BinaryWriter) =
    //actorName: string
    writer.Write actorName
    //protocolName: string
    writer.Write protocolName
    //address: Address
    //custom binary serialization
    address.BinarySerialize(writer)

  static member BinaryDeserialize(reader: BinaryReader): TcpActorId =
    //actorName: string
    let actorName = reader.ReadString()
    //protocolName: string
    let protocolName = reader.ReadString()
    //address: Address
    //custom binary serialization
    let address = Address.BinaryDeserialize(reader)
    new TcpActorId(actorName, protocolName, address)
