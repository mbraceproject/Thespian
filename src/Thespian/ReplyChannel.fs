namespace Nessos.Thespian

    open System.Runtime.Serialization

    open Nessos.Thespian.Utils.Concurrency
    open Nessos.Thespian.Serialization

    type IReplyChannel =
        abstract Protocol: string
        abstract Timeout: int with get, set
        abstract ReplyUntyped: Result<obj> -> unit
        abstract AsyncReplyUntyped: Result<obj> -> Async<unit>

    and IReplyChannel<'T> =
        inherit IReplyChannel
        abstract Reply: Result<'T> -> unit
        abstract AsyncReply: Result<'T> -> Async<unit>
        abstract WithTimeout: int -> IReplyChannel<'T>

    and IReplyChannelFactory =
        abstract Protocol: string
        abstract Filter: IReplyChannel<'T> -> bool
        abstract Create: unit -> ReplyChannelProxy<'T>

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
                    | :? MessageSerializationContext as c when c.ForeignFilter(self.realReplyChannel) ->
                        let nativeReplyChannel : ReplyChannelProxy<'T> = c.CreateReplyChannelOverride<'T>()
                        let nrc = nativeReplyChannel :> IReplyChannel<'T>
                        c.AddReplyChannelOverride(self.realReplyChannel, nativeReplyChannel.realReplyChannel)
                        nrc.Timeout <- self.realReplyChannel.Timeout
                        nrc
                    | _ -> self.realReplyChannel

                info.AddValue("realReplyChannel", serializedReplyChannel)

    and MessageSerializationContext(serializer: IMessageSerializer, replyChannelFactory: IReplyChannelFactory) =
        //list of foreign reply channel information gathered by the context
        //list of (foreignReplyChannel, nativeOverrideReplyChannel)
        let replyChannels = Atom.create List.empty<IReplyChannel * IReplyChannel>
        member __.Serializer = serializer
        member __.ReplyProtocol = replyChannelFactory.Protocol
        member __.ForeignFilter<'T>(rc: IReplyChannel<'T>) = replyChannelFactory.Filter(rc)
        member __.CreateReplyChannelOverride<'T>() = replyChannelFactory.Create<'T>()
        member __.ReplyChannelOverrides with get() = replyChannels.Value
        member __.AddReplyChannelOverride(foreignReplyChannel: IReplyChannel, nativeReplyChannel: IReplyChannel) =
            Atom.swap replyChannels (fun rcs -> (foreignReplyChannel, nativeReplyChannel)::rcs)

        member self.GetStreamingContext() = new StreamingContext(StreamingContextStates.All, self)


    [<AutoOpen>]
    module ReplyChannel =
        //convenience active pattern for getting a reply func
        let (|R|) (replyChannel: IReplyChannel<'T>) = replyChannel.Reply

        module ReplyChannel =
            let map (mapF: 'U -> 'T) (replyChannel: IReplyChannel<'T>): IReplyChannel<'U> =
                { new IReplyChannel<'U> with
                    override __.Protocol = replyChannel.Protocol
                    override __.Timeout with get() = replyChannel.Timeout
                                         and set(timeout) = replyChannel.Timeout <- timeout
                    override self.WithTimeout(timeout) = self.Timeout <- timeout; self
                    override __.ReplyUntyped(reply) = replyChannel.ReplyUntyped(Result.map (unbox >> mapF >> box) reply)
                    override __.AsyncReplyUntyped(reply) = replyChannel.AsyncReplyUntyped(Result.map (unbox >> mapF >> box) reply)
                    override __.Reply(reply) = replyChannel.Reply(Result.map mapF reply)
                    override __.AsyncReply(reply) = replyChannel.AsyncReply(Result.map mapF reply)
                }