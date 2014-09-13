namespace Nessos.Thespian.Remote.TcpProtocol

open System
open System.IO
open System.Net
open System.Net.Sockets
open Nessos.Thespian
open Nessos.Thespian.Utils
open Nessos.Thespian.Utils.Async
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.SocketExtensions

[<Struct; CustomComparison; CustomEquality>]
type HostOrAddress(hostnameOrAddress: string) =
    static let getIps =
        memoize (fun addr ->
            Dns.GetHostAddresses(addr) 
                |> Seq.filter (fun addr -> addr.AddressFamily = AddressFamily.InterNetwork)
                |> Seq.map (fun addr -> addr.ToString())
                |> Set.ofSeq)

    member __.Value = hostnameOrAddress

    member self.Compare(other: HostOrAddress) =
        match self.Value.CompareTo(other.Value) with
        | 0 -> 0
        | stringCmp ->
            let ips = getIps self.Value
            let otherIps = getIps other.Value
            if Set.intersect ips otherIps |> Set.isEmpty |> not then 0
            else stringCmp

    override __.ToString() = hostnameOrAddress

    override __.GetHashCode() = hostnameOrAddress.GetHashCode()
    override self.Equals(other: obj) =
        match other with
        | :? HostOrAddress as ha -> self.Compare(ha) = 0
        | _ -> false

    interface IComparable with
        override self.CompareTo(other: obj) =
            match other with
            | :? HostOrAddress as ha -> self.Compare(ha)
            | _ -> invalidArg "other" "Cannot compare objects of incompatible types."

    interface IComparable<HostOrAddress> with
        override self.CompareTo(other: HostOrAddress) = self.Compare(other)

type Address(hostnameOrAddress : string, ?port : int) =
    let port = defaultArg port 0
    let toString = hostnameOrAddress + ":" + port.ToString()

    static let toEndPointsAsync (hostnameOrAddress: string, port: int) = async {
        if hostnameOrAddress = IPAddress.Any.ToString() then return [new IPEndPoint(IPAddress.Any, port)]
        else 
            let! ips = Dns.AsyncGetHostAddresses(hostnameOrAddress)
            return ips |> Seq.filter (fun addr -> addr.AddressFamily = AddressFamily.InterNetwork)
                       |> Seq.map (fun addr -> new IPEndPoint(addr, port))
                       |> Seq.toList
    }

    static let memoizedToEndPointsAsync = Async.memoize toEndPointsAsync

    do if port < IPEndPoint.MinPort || port > IPEndPoint.MaxPort then
        invalidArg "Address port out of range." "port"

    member __.BinarySerialize(writer: BinaryWriter) =
        //hostnameOrAddress: string
        writer.Write hostnameOrAddress
        //port: int
        writer.Write port

    static member BinaryDeserialize(reader: BinaryReader): Address =
        //hostnameOrAddress: string
        let hostnameOrAddress = reader.ReadString()
        //port: int
        let port = reader.ReadInt32()
        new Address(hostnameOrAddress, port)

    member __.HostnameOrAddress = hostnameOrAddress
    member __.Port = port

    member private self.CompareHostOrAddrs(otherAddress: Address): int =
        match self.HostnameOrAddress.CompareTo(otherAddress.HostnameOrAddress) with
        | 0 -> 0
        | stringCmp ->
            let ips = self.GetIPAddresses()
            let otherIps = otherAddress.GetIPAddresses()
            if Set.intersect ips otherIps |> Set.isEmpty |> not then 0
            else stringCmp

    member self.CompareTo(otherAddress: Address): int =
        compareOn (fun (ha: Address) -> HostOrAddress ha.HostnameOrAddress, ha.Port) self otherAddress

    override __.ToString() = toString

    member self.ToEndPoints() = 
        if self.HostnameOrAddress = IPAddress.Any.ToString() then [new IPEndPoint(IPAddress.Any, self.Port)]
        else Dns.GetHostAddresses(self.HostnameOrAddress)
             |> Seq.filter (fun addr -> addr.AddressFamily = AddressFamily.InterNetwork)
             |> Seq.map (fun addr -> new IPEndPoint(addr, self.Port))
             |> Seq.toList

    member self.ForceToEndPointsAsync() = toEndPointsAsync (self.HostnameOrAddress, self.Port)

    member self.ToEndPointsAsync() = memoizedToEndPointsAsync (self.HostnameOrAddress, self.Port)

    member internal self.GetIPAddresses() = 
        Dns.GetHostAddresses(self.HostnameOrAddress) 
            |> Seq.filter (fun addr -> addr.AddressFamily = AddressFamily.InterNetwork)
            |> Seq.map (fun addr -> addr.ToString())
            |> Set.ofSeq

    override self.GetHashCode() = Unchecked.hash (self.ToString())
    override self.Equals(other: obj) =
        match other with
        | :? Address as otherAddress -> self.CompareTo(otherAddress) = 0
        | _ -> false

    interface IComparable<Address> with
        override self.CompareTo(otherAddress: Address): int = self.CompareTo(otherAddress)

    interface IComparable with
        override self.CompareTo(other: obj): int =
            match other with
            | :? Address as otherAddress -> self.CompareTo(otherAddress)
            | _ -> invalidArg "other" "Cannot compare objects of incompatible types."

    static member Any = Address(IPAddress.Any.ToString(), 0)
    static member LoopBack = Address(IPAddress.Loopback.ToString(), 0)
    static member AnyPort(hostnameOrAddress: string) = Address(hostnameOrAddress, 0)
    static member AnyHost(port: int) = Address(IPAddress.Any.ToString(), port)

    static member TryParse(addressString: string): Address option = 
        match addressString with
        | RegExp.Match "(.+):(\d+)$" (hostnameOrAddress::portString::[]) ->
            match Int32.TryParse portString with
            | (true, port) -> Some <| Address(hostnameOrAddress, port)
            | _ -> None
        | _ -> None

    static member Parse(addressString: string): Address =
        match Address.TryParse addressString with
        | Some address -> address
        | None -> invalidArg "addressString" "Not a valid address string."

    //NOTE!!! Address.FromEndPoint(Address.ToEndPoint()) does not yield the original address
    static member FromEndPoint(endPoint: IPEndPoint) = Address(endPoint.Address.ToString(), endPoint.Port)

module IPEndPoint =
    let any = new IPEndPoint(IPAddress.Any, 0)
    let anyIp port = new IPEndPoint(IPAddress.Any, port)
    let anyPort (ipAddress: IPAddress) = new IPEndPoint(ipAddress, 0)

module AddressUtils =
    let isIpAddressAny (addr : Address) = addr.HostnameOrAddress = IPAddress.Any.ToString()

    let (|AnyIp|_|) address = if isIpAddressAny address then Some() else None
    let (|Any|_|) = function AnyIp -> Some() | _ -> None
    let (|NotIp|_|) (addr : Address) =
        match IPAddress.TryParse(addr.HostnameOrAddress) with
        | (false, _) -> Some()
        | _ -> None

    let (|AnyPort|_|) (addr: Address) = if addr.Port = 0 then Some() else None

    let (|Address|) (address : Address) = address.HostnameOrAddress
    let (|Port|) (address : Address) = address.Port

    let changePort port (address : Address) = Address(address.HostnameOrAddress, port)
    let changeHost host (address : Address) = Address(host, address.Port)

    let getAllEndPoints (address : Address) =
        Dns.GetHostAddresses(address.HostnameOrAddress) |> Seq.filter (fun addr -> addr.AddressFamily = AddressFamily.InterNetwork)
        |> Seq.map (fun addr -> new IPEndPoint(addr, address.Port))
        |> Seq.toList

    let (|AddressOfString|_|) = Address.TryParse

