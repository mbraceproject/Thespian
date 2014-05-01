module Nessos.Thespian.Cluster.ClusterProxy

open System
open Nessos.Thespian
open Nessos.Thespian.PowerPack

type private LogLevel = Nessos.Thespian.LogLevel

// TODO: Thespian's Atom should not be part of the public API

let clusterProxyBehavior (proxyMap: Atom<Map<ActivationReference, ReliableActorRef<RawProxy>>>) (ctx: BehaviorContext<_>) (msg: ClusterProxyManager) =
    async {
        match msg with
        | ForwardMessage(activationRef, payload) ->
            match proxyMap.Value.TryFind activationRef with
            | Some actorRef -> 
                try
                    //FaultPoint
                    //e => forwarding failed ;; report
                    actorRef <-- PostRaw payload
                with e -> ctx.LogError e
            | None ->
                ctx.LogEvent(LogLevel.Error, sprintf "No activation found: %A" activationRef)

        | ForwardMessageSync(RR ctx reply, activationRef, payload) ->
            try
                //Throws
                //KeyNotFoundException => reply back
                let actorRef = proxyMap.Value.[activationRef]
                    
                //FaultPoint
                //e => reply the exception
                actorRef <-- PostRaw payload
                
                reply nothing
            with e ->
                reply <| Exception e

        | ForwardMessageWithReply(RR ctx reply, activationRef, payload) ->
            try
                //Throws
                //KeyNotFoundException => reply back
                let actorRef = proxyMap.Value.[activationRef]
               
                let msgF = unbox (Raw.toMessage payload)
                    
                //FaultPoint
                //e => reply the exception
                let! r = actorRef <!- msgF
                reply (Value r)
            with e ->
                reply <| Exception e

        | ForwardDirect(activationRef, payload) ->
            try
                //Throws
                //KeyNotFoundException => reply back
                let actorRef = proxyMap.Value.[activationRef]
                match payload with
                | PostRaw _ -> actorRef <-- payload
                | PostWithReplyRaw(RR ctx reply, payload) ->
                    let! r = actorRef <!- fun ch -> PostWithReplyRaw(ch, payload)
                    reply (Value r)
            with e -> ctx.LogError e

        | RegisterProxy(activationRef, actorRef) ->
            Atom.swap proxyMap (Map.add activationRef (ReliableActorRef.FromRef actorRef))

        | UnregisterProxy activationRef ->
            Atom.swap proxyMap (Map.remove activationRef)
    }

