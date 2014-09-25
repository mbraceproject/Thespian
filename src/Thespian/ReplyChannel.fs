namespace Nessos.Thespian

open System.Runtime.Serialization

open Nessos.Thespian.Utils
open Nessos.Thespian.Utils.Concurrency
open Nessos.Thespian.Utils.Control
open Nessos.Thespian.Serialization

/// Represents a reply given to a channel
type Reply<'T> = 
    /// Successful reply
    | Value of 'T
    /// Exceptional reply
    | Exn of exn
with
    /// <summary>
    ///     Gets the contained value of given reply.
    /// </summary>
    /// <param name="preserveStackTrace">Keep remote stack trace if exception. Defaults to true.</param>
    member self.GetValue(?preserveStackTrace) : 'T =
        match self with
        | Value t -> t
        | Exn e when defaultArg preserveStackTrace true -> reraise' e
        | Exn e -> raise e

/// Represents an untyped reply given to a channel
type Reply = Reply<obj>

/// Abstract type used in actor APIs that support replies.
type IReplyChannel =
    /// Protocol name for given reply channel
    abstract Protocol: string
    /// Gets or sets the timeout for reply in milliseconds
    abstract Timeout: int with get, set

    /// <summary>
    ///     Asynchronously send a reply to the channel.
    /// </summary>
    /// <param name="reply">Untyped reply value.</param>
    abstract AsyncReplyUntyped : reply:Reply -> Async<unit>

/// Abstract type used in actor APIs that support replies.
type IReplyChannel<'T> =
    inherit IReplyChannel

    /// <summary>
    ///     Asynchronously send a reply to the channel.
    /// </summary>
    /// <param name="reply">Untyped reply value.</param>
    abstract AsyncReply : reply:Reply<'T> -> Async<unit>

/// Reply channel factory abstraction
type IReplyChannelFactory =
        
    /// Protocol implementation name.
    abstract Protocol: string

    /// Initializes a new reply channel for given implementation.
    abstract Create: unit -> ReplyChannelProxy<'T>
        
    /// <summary>
    ///     Decides whether given reply channel is compatible
    ///     with given reply channel protocol implementation.
    /// </summary>
    /// <param name="rc">Reply channel to be examined.</param>
    abstract IsForeignChannel : rc:IReplyChannel<'T> -> bool

/// Used for proxying reply channels
and ReplyChannelProxy<'T> =
    val realReplyChannel: IReplyChannel<'T>

    /// <summary>
    ///     Create a new proxy for given reply channel.
    /// </summary>
    /// <param name="realReplyChannel">source reply channel.</param>
    new(realReplyChannel: IReplyChannel<'T>) = { realReplyChannel = realReplyChannel }
        
    private new (info: SerializationInfo, context: StreamingContext) = 
        { realReplyChannel = info.Read<IReplyChannel<'T>> "realReplyChannel" }

    interface IReplyChannel<'T> with
        override self.Protocol = self.realReplyChannel.Protocol
        override self.Timeout 
            with get() = self.realReplyChannel.Timeout 
            and set timeout = self.realReplyChannel.Timeout <- timeout

        override self.AsyncReplyUntyped reply = self.realReplyChannel.AsyncReplyUntyped reply
        override self.AsyncReply reply = self.realReplyChannel.AsyncReply reply

    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            let serializedReplyChannel =
                match context.Context with
                | :? MessageSerializationContext as c when c.ForeignFilter self.realReplyChannel ->
                    let nativeReplyChannel : ReplyChannelProxy<'T> = c.CreateReplyChannelOverride<'T>()
                    let nrc = nativeReplyChannel :> IReplyChannel<'T>
                    c.AddReplyChannelOverride(self.realReplyChannel, nativeReplyChannel.realReplyChannel)
                    nrc.Timeout <- self.realReplyChannel.Timeout
                    nrc
                | _ -> self.realReplyChannel

            info.Write "realReplyChannel" serializedReplyChannel

/// StreamingContext payload used for gathering reply channel metadata in serialized messages
and MessageSerializationContext(serializer: IMessageSerializer, replyChannelFactory: IReplyChannelFactory) =
    //list of foreign reply channel information gathered by the context
    //list of (foreignReplyChannel, nativeOverrideReplyChannel)
    let replyChannels = Atom.create List.empty<IReplyChannel * IReplyChannel>

    /// Serializer implementation used for context.
    member __.Serializer = serializer

    /// ReplyChannel protocol name
    member __.ReplyProtocol = replyChannelFactory.Protocol

    /// <summary>
    ///     Decides if given reply channel requires patching.
    /// </summary>
    /// <param name="rc">reply channel to be examined.</param>
    member __.ForeignFilter<'T>(rc: IReplyChannel<'T>) = replyChannelFactory.IsForeignChannel rc

    /// Creates a fresh reply channel for native protocol implementation.
    member __.CreateReplyChannelOverride<'T>() = replyChannelFactory.Create<'T>()

    /// Gets the currently registered reply channel overrides
    member __.ReplyChannelOverrides = replyChannels.Value

    /// <summary>
    ///     Appends new reply channel override to the context state.
    /// </summary>
    /// <param name="foreignReplyChannel">Foreign reply channel.</param>
    /// <param name="nativeReplyChannel">Native reply channel.</param>
    member __.AddReplyChannelOverride(foreignReplyChannel: IReplyChannel, nativeReplyChannel: IReplyChannel) =
        Atom.swap replyChannels (fun rcs -> (foreignReplyChannel, nativeReplyChannel)::rcs)

    /// Wraps this instance as a StreamingContext value.
    member self.GetStreamingContext() = new StreamingContext(StreamingContextStates.All, self)


/// Reply Channel public extension methods
[<AutoOpen>]
module ReplyChannel =

    type IReplyChannel<'T> with
        /// <summary>
        ///     Posts a value to reply channel.
        /// </summary>
        /// <param name="value">Value to be posted.</param>
        member rc.Reply (value : 'T) = rc.AsyncReply <| Value value

        /// <summary>
        ///     Posts an exception to reply channel; to be raised at the sender callsite.
        /// </summary>
        /// <param name="exn">Exception to be posted.</param>
        member rc.ReplyWithException (exn : #exn) = rc.AsyncReply <| Exn exn

        /// <summary>
        ///     Synchronously posts a reply to channel
        /// </summary>
        /// <param name="reply">reply value.</param>
        member rc.ReplySynchronously (reply : Reply<'T>) = rc.AsyncReply reply |> Async.RunSynchronously

        /// <summary>
        ///     Updates the timeout for given reply channel.
        /// </summary>
        /// <param name="timeoutMilliseconds">timeout to be set.</param>
        member rc.WithTimeout (timeoutMilliseconds:int) =
            rc.Timeout <- timeoutMilliseconds
            rc

    /// Reply type auxiliary functions
    [<RequireQualifiedAccess>]
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module Reply =

        /// <summary>
        ///     Declare a succesful result.
        /// </summary>
        /// <param name="value">Result value.</param>
        let inline value<'T> (value : 'T) = Reply<'T>.Value value

        /// <summary>
        ///     Declare an exceptional result.
        /// </summary>
        /// <param name="exn">Exception</param>
        let inline exn<'T> (exn : exn) = Reply<'T>.Exn exn

        /// <summary>
        ///     Protects a delayed computation.
        /// </summary>
        /// <param name="f">Computation to be evaluated.</param>
        let protect (f : unit -> 'T) =
            try f () |> Value with e -> Exn e

        /// <summary>
        ///     Result map combinator.
        /// </summary>
        /// <param name="mapF">Map function.</param>
        /// <param name="tresult">Input result.</param>
        let map (mapF : 'T -> 'S) (tresult : Reply<'T>) =
            match tresult with
            | Value x -> Value (mapF x)
            | Exn e -> Exn e

        /// <summary>
        ///     Map function that catches any exception raised by continuation.
        /// </summary>
        /// <param name="f">Continuation function.</param>
        /// <param name="tresult">Input result.</param>
        let bind (f : 'T -> 'S) (tresult : Reply<'T>) =
            match tresult with
            | Value x -> try Value <| f x with e -> Exn e
            | Exn e -> Exn e

        /// <summary>
        ///     Boxes a result value.
        /// </summary>
        /// <param name="result">Result value.</param>
        let box (result : Reply<'T>) : Reply =
            match result with
            | Value t -> Value (box t)
            | Exn e -> Exn e

        /// <summary>
        ///     Unboxes a result value.
        /// </summary>
        /// <param name="result">Result value.</param>
        let unbox<'T> (result : Reply) =
            match result with
            | Value o -> Value (unbox<'T> o)
            | Exn e -> Exn e

    /// Reply channel combinators
    [<RequireQualifiedAccess>]
    module ReplyChannel =

        /// <summary>
        ///     ReplyChannel map combinator
        /// </summary>
        /// <param name="mapF">map function.</param>
        /// <param name="replyChannel">Initial reply channel.</param>
        let map (mapF: 'U -> 'T) (trc: IReplyChannel<'T>): IReplyChannel<'U> =
            { 
                new IReplyChannel<'U> with
                    override __.Protocol = trc.Protocol
                    override __.Timeout 
                        with get() = trc.Timeout
                        and set(timeout) = trc.Timeout <- timeout

                    override __.AsyncReplyUntyped reply = 
                        trc.AsyncReplyUntyped(Reply.map (unbox >> mapF >> box) reply)

                    override __.AsyncReply(reply) = 
                        trc.AsyncReply(Reply.map mapF reply)
            }