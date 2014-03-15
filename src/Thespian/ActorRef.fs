namespace Thespian
    
    open System
    open System.Runtime.Serialization

    [<Serializable>]
    type Reply<'T> = 
        | Value of 'T
        | Exception of exn

    type LogLevel =
        | Info
        | Warning
        | Error

    type LogSource =
        | Actor of string * ActorUUID
        | Protocol of string
    
    type Log<'T> = LogLevel * LogSource * 'T
    type Log = Log<obj>

    [<Serializable>]
    type IProtocolConfiguration =
        inherit IComparable<IProtocolConfiguration>
        inherit IComparable
        abstract ProtocolName: string
        abstract Serializer: string option
        abstract CreateProtocolInstances: ActorRef<'T> -> IActorProtocol<'T> []
        abstract CreateProtocolInstances : ActorUUID * string -> IActorProtocol<'T> []
        //abstract TryCreateProtocolInstance : ActorUUID * string -> IActorProtocol<'T>

    and IActorProtocol =
        abstract Configuration: IProtocolConfiguration option
        abstract MessageType: Type
        abstract ProtocolName: string
        abstract ActorUUId: ActorUUID
        abstract ActorName: string
        abstract ActorId: ActorId
        abstract Log: IEvent<Log>
        abstract Start: unit -> unit
        abstract Stop: unit -> unit

    and IActorProtocol<'T> = 
        inherit IActorProtocol

        abstract Post: 'T -> unit
        abstract PostAsync: 'T -> Async<unit>
        abstract PostWithReply: (IReplyChannel<'R> -> 'T) * int -> Async<'R>
        abstract TryPostWithReply: (IReplyChannel<'R> -> 'T) * int -> Async<'R option>
        abstract PostWithReply: (IReplyChannel<'R> -> 'T) -> Async<'R>

    and IPrincipalActorProtocol<'T> =
        inherit IActorProtocol<'T>

        abstract CurrentQueueLength: int
        
        abstract Receive: int -> Async<'T>
        abstract Receive: unit -> Async<'T>
        abstract TryReceive: int -> Async<'T option>
        abstract TryReceive: unit -> Async<'T option>
        abstract Scan: ('T -> Async<'U> option) * int -> Async<'U>
        abstract Scan: ('T -> Async<'U> option) -> Async<'U>
        abstract TryScan: ('T -> Async<'U> option) * int -> Async<'U option>
        abstract TryScan: ('T -> Async<'U> option) -> Async<'U option>

        abstract Start: (unit -> Async<unit>) -> unit

    and internal ReplyChannelUtils private () =
        static member Map (mapF: 'U -> 'T) (replyChannel: IReplyChannel<'T>): IReplyChannel<'U> =
            {
                new IReplyChannel<'U> with
                    member r.Protocol = replyChannel.Protocol
                    member r.Timeout with get() = replyChannel.Timeout
                                     and set timeout = replyChannel.Timeout <- timeout
                    member r.WithTimeout(timeout) = r.Timeout <- timeout; r
                    member r.ReplyUntyped(reply) = 
                        replyChannel.ReplyUntyped(match reply with Value(:? 'U as value) -> Value(mapF value |> box) | Exception e -> Reply.Exception e | _ -> invalidArg "Reply object not of proper type." "reply")
                    member r.Reply(reply) =
                        replyChannel.Reply(match reply with Value value -> Value(mapF value) | Exception e -> Reply.Exception e)
            }

    and [<Serializable>] [<AbstractClass>] ActorRef = 
        
        val private uuid: ActorUUID
        val private name: string
        val private messageType: Type
        val private protocols: string[]

        new (uuid: ActorUUID, actorName: string, actorMessageType: Type, actorProtocols: seq<string>) = 
            if actorProtocols = Seq.empty then
                raise <| new ArgumentException("No actors specified for actor reference.", "actorProtocols")

            {
                uuid = uuid; name = actorName; messageType = actorMessageType; protocols = actorProtocols |> Seq.toArray;
            }

        internal new (info: SerializationInfo, context: StreamingContext) =
            {
                uuid = info.GetValue("uuid", typeof<ActorUUID>) :?> ActorUUID
                name = info.GetString("name")
                messageType = info.GetValue("messageType", typeof<Type>) :?> Type
                protocols = info.GetValue("protocolConfigurations", typeof<IProtocolConfiguration[]>) :?> IProtocolConfiguration[] |> Array.map (fun configuration -> configuration.ProtocolName)
            }

        abstract MessageType: Type
        default ref.MessageType = ref.messageType

        abstract Protocols: string[]
        default ref.Protocols = ref.protocols

        abstract UUId: ActorUUID
        default ref.UUId = ref.uuid

        abstract Name: string
        default ref.Name = ref.name

        abstract Id: ActorId

        abstract PostUntyped: obj -> unit
        abstract PostUntypedAsync: obj -> Async<unit>
        abstract PostWithReplyUntyped: (IReplyChannel<obj> -> obj) -> Async<obj>
        abstract PostWithReplyUntyped: (IReplyChannel<obj> -> obj) * int -> Async<obj>
        abstract TryPostWithReplyUntyped: (IReplyChannel<obj> -> obj) * int -> Async<obj option>

        abstract SerializationDestructor: SerializationInfo * StreamingContext -> unit
        default ref.SerializationDestructor(info: SerializationInfo, context: StreamingContext) =
            info.AddValue("uuid", ref.uuid)
            info.AddValue("name", ref.name)
            info.AddValue("messageType", ref.MessageType)

        interface ISerializable with
            override ref.GetObjectData(info: SerializationInfo, context: StreamingContext) =
                ref.SerializationDestructor(info, context)

    and [<Serializable>] ActorRef<'T> =
        inherit ActorRef

        val private defaultActorProtocol: IActorProtocol<'T>

        //val private actorProtocolMap: Map<string, IActorProtocol<'T> list>
        val private actorProtocols: IActorProtocol<'T>[]

        new (uuid: ActorUUID, name: string, actorProtocols: IActorProtocol<'T>[]) = 
            {
                inherit ActorRef(uuid, name, typeof<'T>, actorProtocols |> Seq.map (fun protocol -> protocol.ProtocolName))

                defaultActorProtocol = actorProtocols.[0]
                actorProtocols = actorProtocols
//                actorProtocolMap = actorProtocols |> Seq.map (fun actorProtocol -> actorProtocol.ProtocolName, actorProtocol)
//                                                  |> Seq.groupBy fst
//                                                  |> Seq.map (fun (protocolName, protocols) -> protocolName, protocols |> Seq.map snd |> List.ofSeq)
//                                                  |> Map.ofSeq
            }

        new (clone: ActorRef<'T>) =
            let cloneBase = clone :> ActorRef
            ActorRef<'T>(cloneBase.UUId, cloneBase.Name, clone.actorProtocols |> Array.append [| clone.defaultActorProtocol |])
            //ActorRef<'T>(cloneBase.UUId, cloneBase.Name, clone.actorProtocolMap |> Map.toSeq |> Seq.map snd |> Seq.collect (fun ps -> ps |> Seq.filter (fun p -> p.ProtocolName <> clone.defaultActorProtocol.ProtocolName)) |> Seq.toArray |> Array.append [| clone.defaultActorProtocol |])

        //should be protected; but whatever
        new (info: SerializationInfo, context: StreamingContext) = 
            let id = info.GetValue("uuid", typeof<ActorUUID>) :?> ActorUUID
            let name = info.GetString("name")
            let protocolConfigurations = info.GetValue("protocolConfigurations", typeof<IProtocolConfiguration[]>) :?> IProtocolConfiguration[]
            let protocols = protocolConfigurations |> Array.collect (fun configuration -> configuration.CreateProtocolInstances<'T>(id, name))
            let defaultProtocol = protocols.[0]
            {
                inherit ActorRef(info, context)
                
                actorProtocols = protocols
//                actorProtocolMap = protocols
//                    |> Seq.map (fun actorProtocol -> actorProtocol.ProtocolName, actorProtocol)
//                    |> Seq.groupBy fst
//                    |> Seq.map (fun (protocolName, protocols) -> protocolName, protocols |> Seq.map snd |> List.ofSeq)
//                    |> Map.ofSeq

                defaultActorProtocol = defaultProtocol
            }

        member private ref.ProtocolInstances
            with get() = ref.actorProtocols
                //ref.actorProtocolMap |> Map.toSeq |> Seq.map snd |> Seq.collect id

        member private ref.ActorIdSet =
            ref.ProtocolInstances |> Seq.map (fun protocol -> protocol.ActorId) |> Set.ofSeq

        override ref.Id = ref.defaultActorProtocol.ActorId

        abstract Configurations: IProtocolConfiguration list with get
        default ref.Configurations
            with get(): IProtocolConfiguration list =
                ref.ProtocolInstances |> Seq.map (fun protocol -> protocol.Configuration)
                                      |> Seq.choose id
                                      |> Seq.toList

        abstract Item: string -> ActorRef<'T> with get
        default ref.Item 
            with get(protocol: string): ActorRef<'T> = 
                match ref.TryGetProtocolSpecific(protocol) with
                | Some actorRef -> actorRef
                | None -> raise <| new System.Collections.Generic.KeyNotFoundException("Unknown protocol in ActorRef.")

        abstract ConfigurationFilter: (IProtocolConfiguration -> bool) -> ActorRef<'T>
        default ref.ConfigurationFilter(filterF: IProtocolConfiguration -> bool): ActorRef<'T> =
            let protocols = ref.ProtocolInstances |> Seq.filter (fun protocol -> match protocol.Configuration with Some conf -> filterF conf | None -> false)
                                                  |> Seq.toList
            new ActorRef<'T>(ref.UUId, ref.Name, ref.defaultActorProtocol::protocols |> List.toArray)

        abstract TryGetProtocolSpecific: string -> ActorRef<'T> option
        default ref.TryGetProtocolSpecific(protocol: string): ActorRef<'T> option = 
            let specificProtocols = ref.actorProtocols |> Array.filter (fun p -> p.ProtocolName = protocol)
            if Array.isEmpty specificProtocols then None else Some(new ActorRef<'T>(ref.UUId, ref.Name, specificProtocols))
//            match ref.actorProtocolMap.TryFind(protocol) with
//            | Some protocols -> Some(new ActorRef<'T>(ref.UUId, ref.Name, List.toArray protocols))
//            | None -> None

        abstract TryGetActorIdSpecific: ActorId -> ActorRef<'T> option
        default ref.TryGetActorIdSpecific(actorId: ActorId): ActorRef<'T> option =
            match ref.ProtocolInstances |> Seq.tryFind (fun protocol -> protocol.ActorId = actorId) with
            | Some protocol -> Some(new ActorRef<'T>(ref.UUId, ref.Name, [| protocol |]))
            | None -> None
           
        abstract Post: 'T -> unit 
        default ref.Post(msg: 'T): unit = 
            (ref.defaultActorProtocol : IActorProtocol<'T>).Post(msg)

        abstract PostAsync: 'T -> Async<unit>
        default ref.PostAsync(msg: 'T): Async<unit> =
            (ref.defaultActorProtocol : IActorProtocol<'T>).PostAsync(msg)

        abstract PostWithReply: (IReplyChannel<'R> -> 'T) * int -> Async<'R>
        default ref.PostWithReply(msgBuilder: (IReplyChannel<'R> -> 'T), timeout: int): Async<'R> =
            ref.defaultActorProtocol.PostWithReply(msgBuilder, timeout)

        abstract TryPostWithReply: (IReplyChannel<'R> -> 'T) * int -> Async<'R option>
        default ref.TryPostWithReply(msgBuilder: (IReplyChannel<'R> -> 'T), timeout: int): Async<'R option> =
            ref.defaultActorProtocol.TryPostWithReply(msgBuilder, timeout)

        abstract PostWithReply: (IReplyChannel<'R> -> 'T) -> Async<'R>
        default ref.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T): Async<'R> =
            ref.defaultActorProtocol.PostWithReply(msgBuilder)

        override ref.PostUntyped(msg: obj): unit = ref.Post(unbox msg)
        override ref.PostUntypedAsync(msg: obj): Async<unit> = ref.PostAsync(unbox msg)

        override ref.PostWithReplyUntyped(msgF: IReplyChannel<obj> -> obj): Async<obj> = ref.PostWithReply(fun ch -> unbox (msgF ch))

        override ref.PostWithReplyUntyped(msgF: IReplyChannel<obj> -> obj, timeout: int): Async<obj> =
            ref.PostWithReply((fun ch -> unbox (msgF ch)), timeout)

        override ref.TryPostWithReplyUntyped(msgF: IReplyChannel<obj> -> obj, timeout: int): Async<obj option> =
            ref.TryPostWithReply((fun ch -> unbox (msgF ch)), timeout)

        override ref.ToString() =
            let appendWithNewLineAndInlined str1 str2 = str1 + Environment.NewLine + "\t" + str2
            (sprintf "actor://%O.%s" ref.Id typeof<'T>.Name) + 
                (ref.actorProtocols |> Seq.map (fun p -> p.ToString())
                                    |> Seq.toList
                                    |> List.rev |> List.fold appendWithNewLineAndInlined "")
//            (ref.actorProtocolMap |> Map.toSeq 
//                                  |> Seq.map snd
//                                  |> Seq.map (fun protocol -> protocol.ToString())
//                                  |> Seq.toList
//                                  |> List.rev |> List.fold appendWithNewLineAndInlined "")

        override ref.Equals(other: obj) =
            match other with
            | :? ActorRef<'T> as otherRef -> ref.CompareTo(otherRef) = 0
            | _ -> false

        override ref.GetHashCode() = 
            ref.Id.GetHashCode()

        //ActorRef<'T> comparison:
        //Comparison behavior depends on the availability of the UUID
        //if both UUIDs are available (not eq ActorUUID.Empty)
        //then ActorRef<'T>s are equal iff their UUIDs are equal.
        //Otherwise, the comparsion result is determined by the
        //the available ActorIds. Note that in this case
        //there will be a different ActorId for each protocol configuration 
        //Therefore, in the absence of an UUID, ActorRefs are
        //equal iff they have at least one common ActorId, and
        //when not equal ordering is determined by the ordering
        //of the ActorIds of their respective default protocols.
        abstract CompareTo: ActorRef<'T> -> int
        default ref.CompareTo(other: ActorRef<'T>): int =
            if ref.UUId <> ActorUUID.Empty && other.UUId <> ActorUUID.Empty && ref.UUId = other.UUId then 0
            else
                //equality is when the actorRefs have at least one ActorId in common
                if Set.intersect ref.ActorIdSet other.ActorIdSet |> Set.isEmpty |> not then 0
                else                     
                    let comp = ref.Id.CompareTo(other.Id)
                    comp

        override ref.SerializationDestructor(info: SerializationInfo, context: StreamingContext) =
            let protocolConfigurations = ref.ProtocolInstances |> Seq.choose (fun protocol -> protocol.Configuration)

            if protocolConfigurations |> Seq.isEmpty then
                raise <| new SerializationException("Cannot serialize an ActorRef for non-remote protocols.")

            info.AddValue("protocolConfigurations", protocolConfigurations |> Seq.toArray)

            base.SerializationDestructor(info, context)

//        interface ISerializable with
//            override ref.GetObjectData(info: SerializationInfo, context: StreamingContext) =
//                let protocolConfigurations = ref.ProtocolInstances |> Seq.choose (fun protocol -> protocol.Configuration)
//
//                if protocolConfigurations |> Seq.isEmpty then
//                    raise <| new SerializationException("Cannot serialize an ActorRef for non-remote protocols.")
//
//                info.AddValue("protocolConfigurations", protocolConfigurations |> Seq.toArray)
//
//                base.GetObjectData(info, context)

        interface IComparable<ActorRef<'T>> with
            member ref.CompareTo(other: ActorRef<'T>): int =
                ref.CompareTo(other)

        interface IComparable with
            member ref.CompareTo(other: obj): int =
                match other with
                | :? ActorRef<'T> -> (ref :> IComparable<ActorRef<'T>>).CompareTo(other :?> ActorRef<'T>)
                | _ -> invalidArg "Argument not an ActorRef<>" "other"

    and IReplyChannel =
        abstract Protocol: string
        abstract Timeout: int with get, set
        abstract ReplyUntyped: Reply<obj> -> unit
    and IReplyChannel<'T> =
        inherit IReplyChannel
        abstract Reply: Reply<'T> -> unit
        abstract WithTimeout: int -> IReplyChannel<'T>

namespace Thespian.Serialization
    type IMessageSerializer =
        abstract Name: string
        abstract Serialize : context:obj * obj -> byte []
        abstract Deserialize : context:obj * byte [] -> obj

namespace Thespian
    open System
    open System.Runtime.Serialization
    open Thespian.Serialization

    type IReplyChannelFactory =
        abstract Protocol: string
        abstract Create: unit -> ReplyChannelProxy<'T>

    and MessageSerializationContext(serializer: IMessageSerializer, replyChannelFactory: IReplyChannelFactory, furtherContext: obj) =
        //list of foreign reply channel information gathered by the context
        //list of (foreignReplyChannel, nativeOverrideReplyChannel)
        let replyChannels = Atom.atom List.empty<IReplyChannel * IReplyChannel>
        member c.Serializer = serializer
        member c.ReplyProtocol = replyChannelFactory.Protocol
        member c.CreateReplyChannelOverride<'T>() = replyChannelFactory.Create<'T>()
        member c.ReplyChannelOverrides with get() = replyChannels.Value
        member c.AddReplyChannelOverride(foreignReplyChannel: IReplyChannel, nativeReplyChannel: IReplyChannel) =
            Atom.swap replyChannels (fun rcs -> (foreignReplyChannel, nativeReplyChannel)::rcs)
        member c.FurtherContext = furtherContext

    and ReplyChannelProxy<'T> =
        val realReplyChannel: IReplyChannel<'T>

        new(realReplyChannel: IReplyChannel<'T>) = {
            realReplyChannel = realReplyChannel
        }

        internal new (info: SerializationInfo, context: StreamingContext) = {
            realReplyChannel = info.GetValue("realReplyChannel", typeof<IReplyChannel<'T>>) :?> IReplyChannel<'T>
        }

        interface IReplyChannel<'T> with
            override r.Protocol = r.realReplyChannel.Protocol
            override r.ReplyUntyped(reply) = r.realReplyChannel.ReplyUntyped(reply)
            override r.Timeout with get() = r.realReplyChannel.Timeout and set(timeout) = r.realReplyChannel.Timeout <- timeout
            override r.WithTimeout(timeout: int) = r.realReplyChannel.Timeout <- timeout; r :> IReplyChannel<'T>
            override r.Reply(reply) = r.realReplyChannel.Reply(reply)

        interface ISerializable with
            member r.GetObjectData(info: SerializationInfo, context: StreamingContext) =
                let serializedReplyChannel =
                    match context.Context with
                    | :? MessageSerializationContext as c when r.realReplyChannel.Protocol <> c.ReplyProtocol ->
                        let nativeReplyChannel = c.CreateReplyChannelOverride<'T>()
                        let nrc = nativeReplyChannel :> IReplyChannel<'T>
                        c.AddReplyChannelOverride(r.realReplyChannel, nativeReplyChannel.realReplyChannel)
                        nrc.Timeout <- r.realReplyChannel.Timeout
                        nrc
                    | _ -> r.realReplyChannel

                info.AddValue("realReplyChannel", serializedReplyChannel)


    [<AutoOpen>]
    module Reply =
        //convenience active pattern for getting a reply func
        let (|R|) (replyChannel: IReplyChannel<'T>) =
            replyChannel.Reply

        module Reply =
            let exn (e : #exn) : Reply<'T> = Reply.Exception(e :> exn)

            let box (reply: Reply<'T>): Reply<obj> =
                match reply with
                | Value v -> Value(box v)
                | Exception e -> Reply.Exception e

            let unbox<'T> (reply: Reply<obj>): Reply<'T> =
                match reply with
                | Value v -> Value(unbox<'T> v)
                | Exception e -> Reply.Exception e

    module ReplyChannel =
        let map (mapF: 'U -> 'T) (replyChannel: IReplyChannel<'T>): IReplyChannel<'U> = ReplyChannelUtils.Map mapF replyChannel

    //Helper type to be extended with extension methods for giving protocol configurations
    type Protocols private () = class end

