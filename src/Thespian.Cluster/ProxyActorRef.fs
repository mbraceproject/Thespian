namespace Nessos.Thespian.Cluster

open System
open System.Runtime.Serialization
open Nessos.Thespian
open Nessos.Thespian.ActorExtensions

[<Serializable>]
type RawActorRef<'T> =
    inherit ActorRef<'T>

    val rawProxyRef: ActorRef<RawProxy>

    new (rawProxy: ActorRef<RawProxy>) = {
        inherit ActorRef<'T>(rawProxy.UUId, rawProxy.Name, rawProxy.Configurations |> List.map (fun conf -> conf.CreateProtocolInstances(rawProxy.UUId, rawProxy.Name)) |> List.toArray |> Array.concat)
        rawProxyRef = rawProxy
    }

    new (info: SerializationInfo, context: StreamingContext) = {
        inherit ActorRef<'T>(info, context)
        rawProxyRef = info.GetValue("rawProxyRef", typeof<ActorRef<RawProxy>>) :?> ActorRef<RawProxy>
    }

    member ref.RawProxy = ref.rawProxyRef

    override ref.Post(msg: 'T) =
        ref.rawProxyRef <-- (PostRaw <| Raw.fromMessage (box msg))

    override ref.PostAsync(msg: 'T): Async<unit> =
        ref.rawProxyRef.PostAsync(PostRaw <| Raw.fromMessage (box msg))

    override ref.PostWithReply(msgF: IReplyChannel<'R> -> 'T): Async<'R> = 
        let msgF' (ch: IReplyChannel<obj>) = msgF (ReplyChannel.map box ch)
        async {
            let! r = ref.rawProxyRef <!- fun ch -> PostWithReplyRaw(ch, Raw.fromMessage(box msgF'))

            let r' = Raw.toMessage r

            return unbox r'
        }

    override ref.PostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int): Async<'R> =
        let msgF' (ch: IReplyChannel<obj>) = msgF (ReplyChannel.map box ch)
        async {
            let! r = ref.rawProxyRef <!- fun ch -> PostWithReplyRaw(ch.WithTimeout timeout, Raw.fromMessage(box msgF'))

            let r' = Raw.toMessage r

            return unbox r'
        }

    override ref.TryPostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int): Async<'R option> =
        let msgF' (ch: IReplyChannel<obj>) = msgF (ReplyChannel.map box ch)
        async {
            let! r = ref.rawProxyRef.TryPostWithReply((fun ch -> PostWithReplyRaw(ch, Raw.fromMessage(box msgF'))), timeout)

            return match r with
                   | Some rv -> unbox (Raw.toMessage rv)
                   | None -> None
        }

    override ref.SerializationDestructor(info: SerializationInfo, context: StreamingContext) =
        info.AddValue("rawProxyRef", ref.rawProxyRef)

        base.SerializationDestructor(info, context)

    static member FromRawProxy<'T>(rawProxy: ActorRef<RawProxy>) = new RawActorRef<'T>(rawProxy) :> ActorRef<'T>

[<Serializable>]
type ProxyActorRef =
    inherit ActorRef<RawProxy>

    val private clusterProxyManager: ReliableActorRef<ClusterProxyManager>
    val private activationRef: ActivationReference

    new (uuid: ActorUUID, name: string, clusterProxyManager: ReliableActorRef<ClusterProxyManager>, activationRef: ActivationReference) = {
        inherit ActorRef<RawProxy>(uuid, name, clusterProxyManager.UnreliableRef.Configurations |> List.map (fun conf -> conf.CreateProtocolInstances(uuid, name)) |> List.toArray |> Array.concat)
        clusterProxyManager = clusterProxyManager
        activationRef = activationRef
    }

    new (info: SerializationInfo, context: StreamingContext) = {
        inherit ActorRef<RawProxy>(info, context)
        clusterProxyManager = ReliableActorRef.FromRef (info.GetValue("clusterProxyManager", typeof<ActorRef<ClusterProxyManager>>) :?> ActorRef<ClusterProxyManager>)
        activationRef = info.GetValue("activationRef", typeof<ActivationReference>) :?> ActivationReference
    }

    override ref.Post(msg: RawProxy) =
        match msg with
        | PostRaw payload -> ref.clusterProxyManager <-- ForwardMessage(ref.activationRef, payload)
            //ref.clusterProxyManager <!= fun ch -> ForwardMessageSync(ch, ref.activationRef, payload)
        | _ -> ref.clusterProxyManager <-- ForwardDirect(ref.activationRef, msg)

    override ref.PostAsync(msg: RawProxy) =
        match msg with
        | PostRaw payload -> ref.clusterProxyManager.PostAsync(ForwardMessage(ref.activationRef, payload))
            //ref.clusterProxyManager <!= fun ch -> ForwardMessageSync(ch, ref.activationRef, payload)
        | _ -> ref.clusterProxyManager.PostAsync(ForwardDirect(ref.activationRef, msg))

    override ref.PostWithReply(msgF: IReplyChannel<'R> -> RawProxy): Async<'R> = 
        let msgF' (ch: IReplyChannel<Raw<obj>>) = msgF (ReplyChannel.map box (ReplyChannel.map Raw.fromMessage ch))
        async {
            let! r = ref.clusterProxyManager <!- fun ch -> ForwardMessageWithReply(ch, ref.activationRef, Raw.fromMessage(box msgF'))

            let r' = Raw.toMessage r

            return unbox r'
        }

    override ref.PostWithReply(msgF: IReplyChannel<'R> -> RawProxy, timeout: int): Async<'R> =
        let msgF' (ch: IReplyChannel<Raw<obj>>) = msgF (ReplyChannel.map box (ReplyChannel.map Raw.fromMessage ch))
        async {
            let! r = ref.clusterProxyManager <!- fun ch -> ForwardMessageWithReply(ch.WithTimeout timeout, ref.activationRef, Raw.fromMessage(box msgF'))

            let r' = Raw.toMessage r

            return unbox r'
        }

    override ref.TryPostWithReply(msgF: IReplyChannel<'R> -> RawProxy, timeout: int): Async<'R option> =
        let msgF' (ch: IReplyChannel<Raw<obj>>) = msgF (ReplyChannel.map box (ReplyChannel.map Raw.fromMessage ch))
        async {
            let! r = ref.clusterProxyManager.TryPostWithReply((fun ch -> ForwardMessageWithReply(ch, ref.activationRef, Raw.fromMessage(box msgF'))), timeout)

            return match r with
                   | Some rv -> unbox (Raw.toMessage rv)
                   | None -> None
        }

    override ref.SerializationDestructor(info: SerializationInfo, context: StreamingContext) =
        info.AddValue("clusterProxyManager", ref.clusterProxyManager.UnreliableRef)
        info.AddValue("activationRef", ref.activationRef)

        base.SerializationDestructor(info, context)


//    interface ISerializable with
//        override ref.GetObjectData(info: SerializationInfo, context: StreamingContext) =
//            base.GetObjectData(info, context)
//
//            info.AddValue("clusterProxyManager", ref.clusterProxyManager.UnreliableRef)
//            info.AddValue("activationRef", ref.activationRef)
