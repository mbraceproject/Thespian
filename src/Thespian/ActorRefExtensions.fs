namespace Nessos.Thespian.Remote.TcpProtocol
    
    open System
    open Nessos.Thespian
    open Nessos.Thespian.Utils
    open Nessos.Thespian.Remote.TcpProtocol

    [<AutoOpen>]
    module Constants =
        let UTCP = Unidirectional.ProtocolName
        let BTCP = Bidirectional.ProtocolName

    module Utils =
        open System

        let (|UTCP|_|) (protocolName: string) = if protocolName = Constants.UTCP then Some() else None
        let (|BTCP|_|) (protocolName: string) = if protocolName = Constants.BTCP then Some() else None
        let (|TCP|_|) (protocolName: string) =
            match protocolName with
            | UTCP | BTCP  -> Some()
            | _ -> None
        let (|Port|_|) (port: string) =
            match Int32.TryParse(port) with
            | (true, portNum) -> Some portNum
            | _ -> None

        let (|UUIDFromPart|_|) = TcpActorId.TryUUIDPartToActorUUID
        let (|NameFromPart|) = TcpActorId.NamePartToActorName
    
    module Uri =
        type ActorUri = {
            Protocol: string
            HostnameOrAddress: string
            Port: int
            ActorUUID: ActorUUID
            ActorName: string
            Serializer: string
        } with
            member uri.Uri = UriBuilder(uri.Protocol, uri.HostnameOrAddress, uri.Port, sprintf "%A/%s/%s" uri.ActorUUID uri.ActorName uri.Serializer).Uri
            override uri.ToString() = uri.Uri.ToString()
            static member TryParse(uri: string) =
                match uri with
                | RegExp.Match @"^(.+)://(.+):(\d+)/(.+)/(.+)/(.+)$" ((protocolPart & Utils.TCP)::hostnameOrAddress::Utils.Port(port)::Utils.UUIDFromPart(uuid)::Utils.NameFromPart(name)::serializerName::[]) -> 
                    Some { Protocol = protocolPart; HostnameOrAddress = hostnameOrAddress; Port = port; ActorUUID = uuid; ActorName = name; Serializer = serializerName }
                | _ -> None
            static member Parse(uri: string) =
                match ActorUri.TryParse(uri) with
                | Some actorUri -> actorUri
                | None -> invalidArg "Invalid tcp actor uri." "uri"

        let (|ActorUriOfString|_|) (uri: string) = ActorUri.TryParse(uri)
        let (|StringOfActorUri|) (actorUri: ActorUri) = actorUri.ToString()

    module ActorRef =
        open System
        open System.Net
        open Nessos.Thespian
        open Nessos.Thespian.Utils

        let toUris (actorRef: ActorRef<'T>): string list =
            actorRef.Configurations |> List.collect (function :? TcpProtocolConfiguration as conf -> conf.GetUris(actorRef.UUId, actorRef.Name) | _ -> [])

        let tryToUri (actorRef: ActorRef<'T>): string option =
            match actorRef |> toUris with
            | [] -> None
            | uri::_ -> Some uri

        let toUri (actorRef: ActorRef<'T>): string =
            try
                actorRef |> toUris |> List.head
            with _ -> invalidArg "Actor not properly configured to be converted to a uri." "actorRef"

        let tryFromUri (uri: string): ActorRef<'T> option =
            match uri with
            | Uri.ActorUriOfString actorUri ->
                let address = Address(actorUri.HostnameOrAddress, actorUri.Port)
                let configuration = match actorUri.Protocol with
                                    | Utils.UTCP _ -> new Unidirectional.UTcp(Client [address], actorUri.Serializer) :> IProtocolConfiguration
                                    | Utils.BTCP _ -> new Bidirectional.BTcp(Client [address], actorUri.Serializer) :> IProtocolConfiguration
                                    | _ -> failwith "IMPOSSIBLE FAILURE OCCURED!"

                let protocols = configuration.CreateProtocolInstances<'T>(actorUri.ActorUUID, actorUri.ActorName)

                Some <| new ActorRef<'T>(actorUri.ActorUUID, actorUri.ActorName, protocols)
            | _ -> None

        let fromUri (uri: string): ActorRef<'T> =
            match tryFromUri uri with
            | Some actorRef -> actorRef
            | None -> invalidArg "uri" "Invalid tcp actor uri."
                

        let toEndPoints (actorRef: ActorRef<'T>): IPEndPoint list =
            actorRef.Configurations |> List.collect (function :? TcpProtocolConfiguration as conf -> conf.Addresses |> List.collect (fun addr -> addr.ToEndPoints()) | _ -> [])

        let tryToEndPoint (actorRef: ActorRef<'T>): IPEndPoint option =
            match actorRef |> toEndPoints with
            | [] -> None
            | endPoint::_ -> Some endPoint

        let toEndPoint (actorRef: ActorRef<'T>): IPEndPoint =
            try
                actorRef |> toEndPoints |> List.head
            with e ->
                raise <| ArgumentException("ActorRef is not published on a tcp protocol.", e)


