namespace Nessos.Thespian

open System
open System.Runtime.Serialization

open Nessos.Thespian.Utils
open Nessos.Thespian.Utils.Concurrency
open Nessos.Thespian.Logging
open Nessos.Thespian.Serialization


/// Abstract actor protocol factory.
[<Serializable>]
type IProtocolFactory =
    /// Protocol implementation name.
    abstract ProtocolName: string

    /// <summary>
    ///      Create an actor protocol server instance.
    /// </summary>
    /// <param name="actorName">Name of actor bound to server instance.</param>
    /// <param name="actorRef">Actor reference bound to server instance.</param>
    abstract CreateServerInstance: actorName:string * actorRef:ActorRef<'T> -> IProtocolServer<'T>

    /// <summary>
    ///      Create an actor protocol client instance.
    /// </summary>
    /// <param name="actorName">Name of actor to connect to.</param>
    abstract CreateClientInstance: actorName:string -> IProtocolClient<'T>

/// Abstract actor protocol server.
and IProtocolServer<'T> =
    inherit IDisposable
    
    /// Protocol name of server.
    abstract ProtocolName: string

    /// Recipient actor identifier.
    abstract ActorId: ActorId

    /// Local client instance for current protocol.
    abstract Client: IProtocolClient<'T>

    /// Actor event log.
    abstract Log: IEvent<Log>

    /// Starts the actor protocol server.
    abstract Start: unit -> unit

    /// Stops the actor protocol server.
    abstract Stop: unit -> unit
     
/// Abstract actor protocol client. 
and IProtocolClient<'T> =
    /// Protocol name for client
    abstract ProtocolName: string

    /// Recipient actor identifier.
    abstract ActorId: ActorId

    /// Recipient actor uri.
    abstract Uri: string

    /// Protocol factory for current implementation.
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

    /// <summary>
    ///     Synchronously post message to recipient actor.
    /// </summary>
    /// <param name="message">message to be posted.</param>
    abstract Post: message:'T -> unit

    /// <summary>
    ///     Asynchronously post message to recipient actor.
    /// </summary>
    /// <param name="message">message to be posted.</param>
    abstract AsyncPost: message:'T -> Async<unit>
    
    /// <summary>
    ///     Synchronously post-with-reply to recipient actor.
    ///     Computation blocks until recipient responds to given reply channel.
    /// </summary>
    /// <param name="messageBuilder">Message builder for given reply channel.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
    abstract PostWithReply: messageBuilder:(IReplyChannel<'R> -> 'T) * timeoutMilliseconds:int -> Async<'R>

    /// <summary>
    ///     Asynchronously post-with-reply to recipient actor.
    ///     Computation blocks until recipient responds to given reply channel.
    /// </summary>
    /// <param name="messageBuilder">Message builder for given reply channel.</param>
    /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
    abstract TryPostWithReply: messageBuilder:(IReplyChannel<'R> -> 'T) * timeoutMilliseconds:int -> Async<'R option>

and IPrimaryProtocolServer<'T> =
    inherit IProtocolServer<'T>
    abstract PendingMessages: int
    abstract Receive: int -> Async<'T>
    abstract TryReceive: int -> Async<'T option>
    abstract Start: (unit -> Async<unit>) -> unit

and Default() =
    static member val ReplyReceiveTimeout = 10000 with get, set

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
            protocols = info.GetValue("protocolFactories", typeof<IProtocolFactory[]>) :?> IProtocolFactory[] |> Array.map (fun factory -> factory.ProtocolName)
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

    member self.GetUris() = self.protocols |> Seq.map (fun protocol -> protocol.Uri) |> Seq.filter (fun u -> u <> String.Empty) |> Seq.toList

    member self.IsCollocated = self.protocols |> Seq.exists (fun protocol -> protocol.Factory.IsNone)

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
        let timeout = defaultArg timeout Default.ReplyReceiveTimeout
        self.defaultProtocol.PostWithReply(msgF, timeout)

    abstract TryPostWithReply: (IReplyChannel<'R> -> 'T) * ?timeout: int -> Async<'R option>
    default self.TryPostWithReply(msgF: (IReplyChannel<'R> -> 'T), ?timeout: int): Async<'R option> =
        let timeout = defaultArg timeout Default.ReplyReceiveTimeout
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


//Helper type to be extended with extension methods for giving protocol configurations
type Protocols private () = class end

