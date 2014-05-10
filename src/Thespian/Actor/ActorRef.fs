namespace Nessos.Thespian
    
open System
open System.Threading
open System.Runtime.Serialization

open Nessos.Thespian.Serialization

[<Serializable>]
type IProtocolFactory =
  abstract ProtocolName: string
  abstract CreateServerInstance: string * ActorRef<'T> -> IProtocolServer<'T>
  abstract CreateClientInstance: string -> IProtocolClient<'T>

and IProtocolServer<'T> =
  inherit IDisposable
  abstract ProtocolName: string
  abstract ActorId: ActorId
  abstract Client: IProtocolClient<'T>
  abstract Log: IEvent<Log>
  abstract Start: unit -> unit
  abstract Stop: unit -> unit
      
and IProtocolClient<'T> =
  abstract ProtocolName: string
  abstract ActorId: ActorId
  abstract Factory: IProtocolFactory option
  //Asynchronous message passing
  //Succeeds only if message delivery can be guaranteed.
  //Message delivery means that on the receiving side
  //the message has entered the actor's message queue
  //and can eventually be dequeued via Receive()
  //Exceptions:
  //UnknownRecipientException: message received but the actor recipient cannot be found
  //DeliveryException: message received but unable to process payload (e.g. deserialisation exception)
  //CommunicationTimeout: Timeout when
  //a). establishing connection/channel...
  //b). sending message
  //c). receiving confirmation or failure indication of message delivery
  abstract Post: 'T -> unit
  abstract AsyncPost: 'T -> Async<unit>
  //Synchronous message passing
  abstract PostWithReply: (IReplyChannel<'R> -> 'T) * int -> Async<'R>
  abstract TryPostWithReply: (IReplyChannel<'R> -> 'T) * int -> Async<'R option>

and IPrimaryProtocolServer<'T> =
  inherit IProtocolServer<'T>
  abstract PendingMessages: int
  abstract Receive: int -> Async<'T>
  abstract TryReceive: int -> Async<'T option>
  abstract Start: (unit -> Async<unit>) -> unit

and internal ReplyChannelUtils private () =
  static member Map (mapF: 'U -> 'T) (replyChannel: IReplyChannel<'T>): IReplyChannel<'U> =
    {
      new IReplyChannel<'U> with
        override __.Protocol = replyChannel.Protocol
        override __.Timeout with get() = replyChannel.Timeout
                             and set(timeout) = replyChannel.Timeout <- timeout
        override self.WithTimeout(timeout) = self.Timeout <- timeout; self
        override __.ReplyUntyped(reply) = replyChannel.ReplyUntyped(match reply with Value(:? 'U as value) -> Value(mapF value |> box) | Exception e -> Reply.Exception e | _ -> invalidArg "reply" "Reply object not of proper type.")
        override __.AsyncReplyUntyped(reply) = replyChannel.AsyncReplyUntyped(match reply with Value(:? 'U as value) -> Value(mapF value |> box) | Exception e -> Reply.Exception e | _ -> invalidArg "reply" "Reply object not of proper type.")
        override __.Reply(reply) = replyChannel.Reply(match reply with Value value -> Value(mapF value) | Exception e -> Reply.Exception e)
        override __.AsyncReply(reply) = replyChannel.AsyncReply(match reply with Value value -> Value(mapF value) | Exception e -> Reply.Exception e)
    }

and [<Serializable; AbstractClass>] ActorRef =
  val private name: string
  val private messageType: Type
  val private protocols: string[]

  new (name: string, messageType: Type, protocols: seq<string>) =
    if Seq.isEmpty protocols then invalidArg "protocols" "No actors specified for actor reference."
    {
      name = name; messageType = messageType; protocols = Seq.toArray protocols
    }

  internal new (info: SerializationInfo, context: StreamingContext) =
    {    
      name = info.GetString("name")
      messageType = info.GetValue("messageType", typeof<Type>) :?> Type
      protocols = info.GetValue("protocols", typeof<IProtocolFactory[]>) :?> IProtocolFactory[] |> Array.map (fun configuration -> configuration.ProtocolName)
    }

  abstract MessageType: Type
  default self.MessageType = self.messageType

  abstract Protocols: string[]
  default self.Protocols = self.protocols

  abstract Name: string
  default self.Name = self.name

  abstract Id: ActorId

  abstract PostUntyped: obj -> unit
  abstract AsyncPostUntyped: obj -> Async<unit>
  abstract PostWithReplyUntyped: (IReplyChannel<obj> -> obj) * ?timeout: int -> Async<obj>
  abstract TryPostWithReplyUntyped: (IReplyChannel<obj> -> obj) * ?timeout: int -> Async<obj option>

  abstract SerializationDestructor: SerializationInfo * StreamingContext -> unit
  default self.SerializationDestructor(info: SerializationInfo, context: StreamingContext) =
    info.AddValue("name", self.name)
    info.AddValue("messageType", self.MessageType)

  interface ISerializable with
    override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
      self.SerializationDestructor(info, context)

and [<Serializable>] ActorRef<'T> =
  inherit ActorRef

  val private defaultProtocol: IProtocolClient<'T>
  val private protocols: IProtocolClient<'T>[]
  val private protocolFactories: IProtocolFactory[]

  new (name: string, protocols: IProtocolClient<'T>[]) =
    {
      inherit ActorRef(name, typeof<'T>, protocols |> Seq.map (fun protocol -> protocol.ProtocolName))

      defaultProtocol = protocols.[0]
      protocols = protocols
      protocolFactories = protocols |> Array.choose (fun protocol -> protocol.Factory)
    }

  new (clone: ActorRef<'T>) = new ActorRef<'T>((clone :> ActorRef).Name, clone.protocols)
  
  //should be protected; but whatever
  new (info: SerializationInfo, context: StreamingContext) =
    let name = info.GetString("name")
    let protocolFactories = info.GetValue("protocolFactories", typeof<IProtocolFactory[]>) :?> IProtocolFactory[]
    let protocols = protocolFactories |> Array.map (fun factory -> factory.CreateClientInstance<'T>(name))
    let defaultProtocol = protocols.[0]
    {
      inherit ActorRef(info, context)

      defaultProtocol = defaultProtocol
      protocols = protocols
      protocolFactories = protocols |> Array.choose (fun protocol -> protocol.Factory)
    }

  member private self.ProtocolInstances = self.protocols
  member private self.ActorIdSet = self.ProtocolInstances |> Seq.map (fun protocol -> protocol.ActorId) |> Set.ofSeq

  override self.Id = self.defaultProtocol.ActorId

  abstract ProtocolFactories: IProtocolFactory[]
  default self.ProtocolFactories = self.protocolFactories

  abstract Item: string -> ActorRef<'T> with get
  default self.Item
    with get(protocol: string): ActorRef<'T> = 
      match self.TryGetProtocolSpecific(protocol) with
      | Some actorRef -> actorRef
      | None -> raise <| new System.Collections.Generic.KeyNotFoundException("Unknown protocol in ActorRef.")

  abstract ProtocolFilter: (IProtocolFactory -> bool) -> ActorRef<'T>
  default self.ProtocolFilter(filterF: IProtocolFactory -> bool): ActorRef<'T> =
    let protocols = self.ProtocolInstances |> Seq.filter (fun protocol -> match protocol.Factory with Some factory -> filterF factory | None -> false)
                                           |> Seq.toList
    new ActorRef<'T>(self.Name, self.defaultProtocol::protocols |> List.toArray)

  abstract TryGetProtocolSpecific: string -> ActorRef<'T> option
  default self.TryGetProtocolSpecific(protocol: string): ActorRef<'T> option = 
    let specificProtocols = self.protocols |> Array.filter (fun p -> p.ProtocolName = protocol)
    if Array.isEmpty specificProtocols then None else Some(new ActorRef<'T>(self.Name, specificProtocols))

  abstract TryGetActorIdSpecific: ActorId -> ActorRef<'T> option
  default self.TryGetActorIdSpecific(actorId: ActorId): ActorRef<'T> option =
    match self.ProtocolInstances |> Seq.tryFind (fun protocol -> protocol.ActorId = actorId) with
    | Some protocol -> Some(new ActorRef<'T>(self.Name, [| protocol |]))
    | None -> None
           
  abstract Post: 'T -> unit 
  default self.Post(msg: 'T): unit = self.defaultProtocol.Post(msg)

  abstract AsyncPost: 'T -> Async<unit>
  default self.AsyncPost(msg: 'T): Async<unit> = self.defaultProtocol.AsyncPost(msg)

  abstract PostWithReply: (IReplyChannel<'R> -> 'T) * ?timeout: int -> Async<'R>
  default self.PostWithReply(msgF: (IReplyChannel<'R> -> 'T), ?timeout: int): Async<'R> =
    let timeout = defaultArg timeout Timeout.Infinite
    self.defaultProtocol.PostWithReply(msgF, timeout)

  abstract TryPostWithReply: (IReplyChannel<'R> -> 'T) * ?timeout: int -> Async<'R option>
  default self.TryPostWithReply(msgF: (IReplyChannel<'R> -> 'T), ?timeout: int): Async<'R option> =
    let timeout = defaultArg timeout Timeout.Infinite
    self.defaultProtocol.TryPostWithReply(msgF, timeout)

  override self.PostUntyped(msg: obj): unit = self.Post(unbox msg)
  override self.AsyncPostUntyped(msg: obj): Async<unit> = self.AsyncPost(unbox msg)
  override self.PostWithReplyUntyped(msgF: IReplyChannel<obj> -> obj, ?timeout: int): Async<obj> = self.PostWithReply((fun ch -> unbox (msgF ch)), ?timeout = timeout)
  override self.TryPostWithReplyUntyped(msgF: IReplyChannel<obj> -> obj, ?timeout: int): Async<obj option> = self.TryPostWithReply((fun ch -> unbox (msgF ch)), ?timeout = timeout)

  override self.ToString() =
    let appendWithNewLineAndInlined str1 str2 = str1 + Environment.NewLine + "\t" + str2
    (sprintf "actor://%O.%s" self.Id typeof<'T>.Name) +
             (self.protocols |> Seq.map (fun p -> p.ToString())
                             |> Seq.toList
                             |> List.rev |> List.fold appendWithNewLineAndInlined "")

  override self.Equals(other: obj) =
    match other with
    | :? ActorRef<'T> as otherRef -> self.CompareTo(otherRef) = 0
    | _ -> false

  override self.GetHashCode() = self.Id.GetHashCode()

  abstract CompareTo: ActorRef<'T> -> int
  default self.CompareTo(other: ActorRef<'T>): int =
    //equality is when the actorRefs have at least one ActorId in common
    if Set.intersect self.ActorIdSet other.ActorIdSet |> Set.isEmpty |> not then 0
    else let comp = self.Id.CompareTo(other.Id) in comp

  override self.SerializationDestructor(info: SerializationInfo, context: StreamingContext) =
    let protocolFactories = self.ProtocolInstances |> Seq.choose (fun protocol -> protocol.Factory)
    if Seq.isEmpty protocolFactories then raise <| new SerializationException("Cannot serialize an ActorRef for non-remote protocols.")
    info.AddValue("protocolFactories", Seq.toArray protocolFactories)
    base.SerializationDestructor(info, context)

  interface IComparable<ActorRef<'T>> with override self.CompareTo(other: ActorRef<'T>): int = self.CompareTo(other)
  interface IComparable with
    override self.CompareTo(other: obj): int =
      match other with
      | :? ActorRef<'T> -> (self :> IComparable<ActorRef<'T>>).CompareTo(other :?> ActorRef<'T>)
      | _ -> invalidArg "Argument not an ActorRef<>" "other"

and IReplyChannel =
  abstract Protocol: string
  abstract Timeout: int with get, set
  abstract ReplyUntyped: Reply<obj> -> unit
  abstract AsyncReplyUntyped: Reply<obj> -> Async<unit>

and IReplyChannel<'T> =
  inherit IReplyChannel
  abstract Reply: Reply<'T> -> unit
  abstract AsyncReply: Reply<'T> -> Async<unit>
  abstract WithTimeout: int -> IReplyChannel<'T>

and IReplyChannelFactory =
  abstract Protocol: string
  abstract Create: unit -> ReplyChannelProxy<'T>

and MessageSerializationContext(serializer: IMessageSerializer, replyChannelFactory: IReplyChannelFactory) =
  //list of foreign reply channel information gathered by the context
  //list of (foreignReplyChannel, nativeOverrideReplyChannel)
  let replyChannels = Atom.atom List.empty<IReplyChannel * IReplyChannel>
  member __.Serializer = serializer
  member __.ReplyProtocol = replyChannelFactory.Protocol
  member __.CreateReplyChannelOverride<'T>() = replyChannelFactory.Create<'T>()
  member __.ReplyChannelOverrides with get() = replyChannels.Value
  member __.AddReplyChannelOverride(foreignReplyChannel: IReplyChannel, nativeReplyChannel: IReplyChannel) =
    Atom.swap replyChannels (fun rcs -> (foreignReplyChannel, nativeReplyChannel)::rcs)

  member self.GetStreamingContext() = new StreamingContext(StreamingContextStates.All, self)

and ReplyChannelProxy<'T> =
  val realReplyChannel: IReplyChannel<'T>

  new(realReplyChannel: IReplyChannel<'T>) = { realReplyChannel = realReplyChannel }
  internal new (info: SerializationInfo, context: StreamingContext) = { realReplyChannel = info.GetValue("realReplyChannel", typeof<IReplyChannel<'T>>) :?> IReplyChannel<'T> }

  interface IReplyChannel<'T> with
    override self.Protocol = self.realReplyChannel.Protocol
    override self.ReplyUntyped(reply) = self.realReplyChannel.ReplyUntyped(reply)
    override self.AsyncReplyUntyped(reply) = self.realReplyChannel.AsyncReplyUntyped(reply)
    override self.Timeout with get() = self.realReplyChannel.Timeout and set(timeout) = self.realReplyChannel.Timeout <- timeout
    override self.WithTimeout(timeout: int) = self.realReplyChannel.Timeout <- timeout; self :> IReplyChannel<'T>
    override self.Reply(reply) = self.realReplyChannel.Reply(reply)
    override self.AsyncReply(reply) = self.realReplyChannel.AsyncReply(reply)

  interface ISerializable with
    override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
      let serializedReplyChannel =
        match context.Context with
        | :? MessageSerializationContext as c when self.realReplyChannel.Protocol <> c.ReplyProtocol ->
          let nativeReplyChannel = c.CreateReplyChannelOverride<'T>()
          let nrc = nativeReplyChannel :> IReplyChannel<'T>
          c.AddReplyChannelOverride(self.realReplyChannel, nativeReplyChannel.realReplyChannel)
          nrc.Timeout <- self.realReplyChannel.Timeout
          nrc
        | _ -> self.realReplyChannel

      info.AddValue("realReplyChannel", serializedReplyChannel)


[<AutoOpen>]
module Reply =
  //convenience active pattern for getting a reply func
  let (|R|) (replyChannel: IReplyChannel<'T>) = replyChannel.Reply

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

