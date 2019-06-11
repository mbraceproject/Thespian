[<AutoOpen>]
module Nessos.Thespian.Remote.Protocols

open System.Net
open Nessos.Thespian

let UTCP = Nessos.Thespian.Remote.TcpProtocol.Unidirectional.ProtocolName
let BTCP = Nessos.Thespian.Remote.TcpProtocol.Bidirectional.ProtocolName

type Protocols with
    static member utcp(?endPoint: IPEndPoint) =
        let endPoint = defaultArg endPoint (new IPEndPoint(IPAddress.Any, 0))
        new TcpProtocol.Unidirectional.UTcpFactory(TcpProtocol.Unidirectional.ProtocolMode.FromIpEndpoint endPoint) :> IProtocolFactory

    static member btcp(?endPoint: IPEndPoint) =
        let endPoint = defaultArg endPoint (new IPEndPoint(IPAddress.Any, 0))
        new TcpProtocol.Bidirectional.BTcpFactory(TcpProtocol.Bidirectional.ProtocolMode.FromIpEndpoint endPoint) :> IProtocolFactory
