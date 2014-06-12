[<AutoOpen>]
module Nessos.Thespian.Remote.ProtocolFactories

open System
open System.IO
open System.Net
open System.Runtime.Serialization
open Nessos.Thespian
open Nessos.Thespian.Utils
open Nessos.Thespian.Serialization

type Protocols with
  static member utcp(?endPoint: IPEndPoint) =
    let endPoint = defaultArg endPoint (new IPEndPoint(IPAddress.Any, 0))
    new TcpProtocol.Unidirectional.UTcpFactory(TcpProtocol.Unidirectional.ProtocolMode.Server endPoint) :> IProtocolFactory

  static member btcp(?endPoint: IPEndPoint) =
    let endPoint = defaultArg endPoint (new IPEndPoint(IPAddress.Any, 0))
    new TcpProtocol.Bidirectional.BTcpFactory(TcpProtocol.Bidirectional.ProtocolMode.Server endPoint) :> IProtocolFactory

    // type PublishMode =
    //     | Client of Address list
    //     | Server of IPEndPoint list

    // type ProtocolAddressKind =
    //     | ClientAddress of Address
    //     | ServerEndPoint of IPEndPoint

    // [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    // module ProtocolAddressKind =
    //     let isClientAddress = function ClientAddress _ -> true | _ -> false
    //     let isServerEndPoint = not << isClientAddress
    //     let toPublishMode = function ClientAddress address -> Client [address]
    //                                  | ServerEndPoint endPoint -> Server [endPoint]

    // module Publish =
    //     let all = Server []
    //     let endPoints (endpoints: IPEndPoint list) = Server endpoints

    // [<AbstractClass; Serializable>]
    // type TcpProtocolConfiguration =
    //     val private publishMode: PublishMode
    //     val private serializerName: string option

    //     new (publishMode: PublishMode, ?serializerName: string) =
    //         match serializerName with
    //         | Some name when not <| SerializerRegistry.IsRegistered name ->
    //             invalidArg name "Unknown serializer."
    //         | _ -> ()
    //         {
    //             publishMode = publishMode
    //             serializerName = serializerName
    //         }

    //     internal new (info: SerializationInfo, context: StreamingContext) =
    //         let publishMode = info.GetValue("publishMode", typeof<PublishMode>) :?> PublishMode
    //         let serializerName = info.GetValue("serializerName", typeof<string option>) :?> string option
    //         match serializerName with
    //         | Some name when not <| SerializerRegistry.IsRegistered name ->
    //             invalidArg "serializerName" "Unknown serializer."
    //         | _ -> ()
    //         {
    //             publishMode = publishMode
    //             serializerName = serializerName
    //         }

    //     abstract ProtocolName: string
    //     abstract ConstructProtocols: ActorUUID * string * PublishMode * ActorRef<'T> option * string option -> IActorProtocol<'T> []

    //     member tcp.Addresses = 
    //         match tcp.publishMode with
    //         | Client addresses -> addresses
    //         | Server [] ->
    //             TcpListenerPool.GetListeners(IPEndPoint.any, ?serializer = tcp.serializerName)
    //             |> Seq.map (fun listener -> listener.LocalEndPoint)
    //             |> Seq.map (fun ipEndPoint -> new Address(TcpListenerPool.DefaultHostname, ipEndPoint.Port))
    //             |> Seq.toList
    //         | Server endPoints ->
    //             endPoints |> List.map (fun ipEndPoint -> new Address(TcpListenerPool.DefaultHostname, ipEndPoint.Port))

    //     //The format of a uri identifying an actor published on a tcp protocol is as follows
    //     //protocolName://hostNameOrAddress:port/uuidPart/namePart/serializer
    //     //where protocolName      ::= 'utcp' | 'btcp'
    //     //and   hostnameOrAddress ::= hostname | IPAddress (where IPAddress.AddressFamily = AddressFamily.InterNetwork, (IPv4 address))
    //     //and   uuidPart          ::= '*' | ActorUUID
    //     //and   namePart          ::= '*' | string
    //     //
    //     //for a given uri, protocolName/hostNameOrAddress:port/uuidPart/namePart is the ActorId of the actor identified by the uri
    //     member tcp.GetUris(actorUUId: ActorUUID, actorName: string) = 
    //         tcp.Addresses |> List.map (fun addr ->
    //             let uuidPart, namePart = TcpActorId.GetUUIDAndNameParts(actorUUId, actorName)
    //             sprintf "%s://%O/%s/%s/%s" tcp.ProtocolName addr uuidPart namePart (match tcp.serializerName with Some name -> name | _ -> SerializerRegistry.GetDefaultSerializer().Name))            

    //     override tcp.Equals(other: obj): bool =
    //             (tcp :> IComparable).CompareTo(other) = 0
    //     override tcp.GetHashCode(): int =
    //             (tcp.Addresses |> List.sumBy (fun addr -> addr.GetHashCode())) + tcp.ProtocolName.GetHashCode() + tcp.serializerName.GetHashCode() * 47

    //     interface IProtocolConfiguration with
    //         member tcp.Serializer = tcp.serializerName
    //         member tcp.ProtocolName = tcp.ProtocolName
    //         //this is used for creating client/server protocol instances
    //         member tcp.CreateProtocolInstances<'T>(actorRef: ActorRef<'T>): IActorProtocol<'T> [] =
    //             tcp.ConstructProtocols<'T>(actorRef.UUId, actorRef.Name, tcp.publishMode, Some actorRef, tcp.serializerName)
    //         //this is used to create client only protocol instances
    //         member tcp.CreateProtocolInstances<'T>(actorUUID: ActorUUID, actorName: string): IActorProtocol<'T> [] =
    //             tcp.ConstructProtocols<'T>(actorUUID, actorName, tcp.publishMode, None, tcp.serializerName)

    //     interface IComparable with
    //         override r.CompareTo(other: obj): int =
    //             match other with
    //             | :? TcpProtocolConfiguration as otherTcp -> 
    //                 let addressComparison = 
    //                     if r.Addresses > otherTcp.Addresses then 1
    //                     else if r.Addresses < otherTcp.Addresses then -1
    //                     else 0
    //                 if addressComparison = 0 
    //                 then match r.serializerName, (otherTcp :> IProtocolConfiguration).Serializer with
    //                         | Some s1, Some s2 -> s1.CompareTo(s2)
    //                         | Some _, None -> 1
    //                         | None, Some _ -> -1
    //                         | None, None -> 0
    //                 else addressComparison
    //             | _ -> -1

    //     interface IComparable<IProtocolConfiguration> with
    //         override r.CompareTo(other: IProtocolConfiguration): int = (r :> IComparable).CompareTo(other)

    //     interface ISerializable with
    //         override tcp.GetObjectData(info: SerializationInfo, context: StreamingContext) =
    //             info.AddValue("publishMode", Client tcp.Addresses)
    //             info.AddValue("serializerName", tcp.serializerName)
