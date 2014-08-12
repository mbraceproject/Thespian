namespace Nessos.Thespian.Remote
    
open System
open Nessos.Thespian
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Utils

[<AutoOpen>]
module Constants =
  let UTCP = Unidirectional.ProtocolName
  let BTCP = Bidirectional.ProtocolName
  let NPP = Remote.PipeProtocol.Protocol.NPP
  let DefaultTcpPort = 2753
    
module Uri =

  let private (|UTCP|_|) (protocolName: string) = if protocolName = Constants.UTCP then Some() else None
  let private (|BTCP|_|) (protocolName: string) = if protocolName = Constants.BTCP then Some() else None
  let private (|NPP|_|) (protocolName: string) = if protocolName = Constants.NPP then Some() else None
  let private (|TCP|_|) (protocolName: string) =
    match protocolName with
    | UTCP | BTCP  -> Some()
    | _ -> None

  type IUriParser =
    abstract Parse: Uri -> ActorRef<'T>

  type TcpParser internal () =
    interface IUriParser with
      override __.Parse (uri: Uri): ActorRef<'T> =
        let port = if uri.Port = -1 then DefaultTcpPort else uri.Port
        let address = new Address(uri.Host, port)

        let factory = match uri.Scheme with
                      | UTCP -> new Unidirectional.UTcpFactory(Unidirectional.Client address) :> IProtocolFactory
                      | BTCP -> new Bidirectional.BTcpFactory(Unidirectional.Client address) :> IProtocolFactory
                      | _ -> failwith "Used tcp uri parser for non-tcp protocol."

        let actorName = uri.PathAndQuery.Substring(1)

        let protocol = factory.CreateClientInstance<'T>(actorName)

        new ActorRef<'T>(uri.PathAndQuery, [| protocol |])

  type NppParser internal () =
    interface IUriParser with
      override __.Parse (uri: Uri): ActorRef<'T> =
        let processId = uri.Port
        let actorName = uri.PathAndQuery.Substring(1)
        let factory = new Remote.PipeProtocol.PipeProtocolFactory(processId) :> IProtocolFactory

        let protocol = factory.CreateClientInstance<'T>(actorName)

        new ActorRef<'T>(actorName, [| protocol |])
        

  let private initParsers() =
    let tcpParser = new TcpParser() :> IUriParser
    let nppParser = new NppParser() :> IUriParser
    Map.empty |> Map.add UTCP tcpParser
              |> Map.add BTCP tcpParser
              |> Map.add NPP nppParser
  
  type Config private() =
    static let parsers = Atom.atom <| initParsers()
    static member TryGetParser(protocol: string) = parsers.Value.TryFind protocol

module ActorRef =
  open System
  open System.Net
  open Nessos.Thespian
  open Nessos.Thespian.Utils

  let toUris (actorRef: ActorRef<'T>): string list = actorRef.GetUris()

  let toUri (actorRef: ActorRef<'T>): string =
    try actorRef.GetUris() |> List.head
    with :? ArgumentException as e -> raise <| new ArgumentException("ActorRef not supporting uris, perhaps due to an unpublished actor.", "actorRef", e)

  let tryFromUri (uri: string): ActorRef<'T> option =
    let u = new System.Uri(uri, UriKind.Absolute)
    match Uri.Config.TryGetParser u.Scheme with
    | Some parser -> Some (parser.Parse u)
    | None -> None

  let fromUri (uri: string): ActorRef<'T> =
    match tryFromUri uri with
    | Some actorRef -> actorRef
    | None -> invalidArg "uri" "Unknown protocol uri."
