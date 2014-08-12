namespace Nessos.Thespian.Cluster

open System
open System.Runtime.Serialization
open Nessos.Thespian
open Nessos.Thespian.PowerPack

[<Serializable>]
type RawActorRef<'T> =
    inherit ActorRef<'T>

    val rawProxyRef: ActorRef<RawProxy>

    new (rawProxy: ActorRef<RawProxy>) = {
        inherit ActorRef<'T>(rawProxy.Name, rawProxy.ProtocolFactories |> Array.map (fun f -> f.CreateClientInstance(rawProxy.Name)))
        rawProxyRef = rawProxy
    }

    new (info: SerializationInfo, context: StreamingContext) = {
        inherit ActorRef<'T>(info, context)
        rawProxyRef = info.GetValue("rawProxyRef", typeof<ActorRef<RawProxy>>) :?> ActorRef<RawProxy>
    }

    member ref.RawProxy = ref.rawProxyRef

    override ref.Post(msg: 'T) =
        ref.rawProxyRef <-- (PostRaw <| Raw.fromMessage (box msg))

    override ref.AsyncPost(msg: 'T): Async<unit> =
        ref.rawProxyRef.AsyncPost(PostRaw <| Raw.fromMessage (box msg))

    override ref.PostWithReply(msgF: IReplyChannel<'R> -> 'T, ?timeout: int): Async<'R> = 
        let msgF' (ch: IReplyChannel<obj>) = msgF (ReplyChannel.map box ch)
        async {
            let! r = ref.rawProxyRef.PostWithReply((fun ch -> PostWithReplyRaw(ch, Raw.fromMessage(box msgF'))), ?timeout = timeout)

            let r' = Raw.toMessage r

            return unbox r'
        }

    override ref.TryPostWithReply(msgF: IReplyChannel<'R> -> 'T, ?timeout: int): Async<'R option> =
        let msgF' (ch: IReplyChannel<obj>) = msgF (ReplyChannel.map box ch)
        async {
            let! r = ref.rawProxyRef.TryPostWithReply((fun ch -> PostWithReplyRaw(ch, Raw.fromMessage(box msgF'))), ?timeout = timeout)

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

    new (name: string, clusterProxyManager: ReliableActorRef<ClusterProxyManager>, activationRef: ActivationReference) = {
        inherit ActorRef<RawProxy>(name, clusterProxyManager.UnreliableRef.ProtocolFactories |> Array.map (fun f -> f.CreateClientInstance(name)))
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
        | _ -> ref.clusterProxyManager <-- ForwardDirect(ref.activationRef, msg)

    override ref.AsyncPost(msg: RawProxy) =
        match msg with
        | PostRaw payload -> ref.clusterProxyManager.AsyncPost(ForwardMessage(ref.activationRef, payload))
        | _ -> ref.clusterProxyManager.AsyncPost(ForwardDirect(ref.activationRef, msg))

    override ref.PostWithReply(msgF: IReplyChannel<'R> -> RawProxy, ?timeout: int): Async<'R> = 
        let msgF' (ch: IReplyChannel<Raw<obj>>) = msgF (ReplyChannel.map box (ReplyChannel.map Raw.fromMessage ch))
        async {
            let! r = ref.clusterProxyManager.PostWithReply((fun ch -> ForwardMessageWithReply(ch, ref.activationRef, Raw.fromMessage(box msgF'))), ?timeout = timeout)

            let r' = Raw.toMessage r

            return unbox r'
        }

    override ref.TryPostWithReply(msgF: IReplyChannel<'R> -> RawProxy, ?timeout: int): Async<'R option> =
        let msgF' (ch: IReplyChannel<Raw<obj>>) = msgF (ReplyChannel.map box (ReplyChannel.map Raw.fromMessage ch))
        async {
            let! r = ref.clusterProxyManager.TryPostWithReply((fun ch -> ForwardMessageWithReply(ch, ref.activationRef, Raw.fromMessage(box msgF'))), ?timeout = timeout)

            return match r with
                   | Some rv -> unbox (Raw.toMessage rv)
                   | None -> None
        }

    override ref.SerializationDestructor(info: SerializationInfo, context: StreamingContext) =
        info.AddValue("clusterProxyManager", ref.clusterProxyManager.UnreliableRef)
        info.AddValue("activationRef", ref.activationRef)

        base.SerializationDestructor(info, context)
