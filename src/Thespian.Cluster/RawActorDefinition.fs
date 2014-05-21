namespace Nessos.Thespian.Cluster

open Nessos.Thespian
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.ActorExtensions
open Nessos.Thespian.Cluster.BehaviorExtensions
open Nessos.Thespian.Reversible

type IRawProxyDef =
    abstract NormalMessageType: System.Type

type RawProxyActorDefinition<'T>(parent: DefinitionPath, normalDef: ActorDefinition<'T>) =
    inherit ActorDefinition<RawProxy>(parent)

    override __.Name = normalDef.Name + "Raw"
    override __.Configuration = normalDef.Configuration
    override __.GetDependencies(conf: ActivationConfiguration) = 
        [{
            Definition = normalDef.Path
            Instance = None //same as this one
            Configuration = normalDef.Configuration.Override(conf)
            ActivationStrategy = ActivationStrategy.collocated
        }]
    override def.Dependencies = def.GetDependencies(def.Configuration)
    override __.PublishProtocols = normalDef.PublishProtocols
    override __.Behavior(_, instanceId) = async {
        let normal = Cluster.NodeRegistry.ResolveLocal<'T>
                        { Definition = normalDef.Path; InstanceId = instanceId }

        return Behavior.stateless (fun ctx msg -> async {
            try
                match msg with
                | PostRaw raw ->
                    let payload = unbox (Raw.toMessage raw)
                    normal <-- payload
                | PostWithReplyRaw(RR ctx reply, raw) ->
                    let msgF = unbox (Raw.toMessage raw)
                    let! r = normal <!- msgF

                    Raw.fromMessage r
                    |> Value
                    |> reply
            with e -> ctx.LogError e
        })
    }

    //override __.GetActorRef(actor: Actor<RawProxy>) = new RawActorRef<'T>(actor.Ref) :> ActorRef

    interface IRawProxyDef with
        override __.NormalMessageType = typeof<'T>


[<AbstractClass>]
type RawActorDefinition<'T>(parent: DefinitionPath) =
    inherit GroupDefinition(parent)

    abstract PublishProtocols: IProtocolConfiguration list
    override __.PublishProtocols = [new Unidirectional.UTcp()]
    abstract Behavior: ActivationConfiguration * int -> Async<Actor<'T> -> Async<unit>>

    abstract OnActorFailure: int * ActivationConfiguration -> (exn -> unit)
    default __.OnActorFailure(_, _) = ignore

    member def.IsLocal = def.PublishProtocols.IsEmpty

    override def.Collocated =
        let normalDef =
            {
                new ActorDefinition<'T>(def.Path) with
                    override __.Name = def.Name
                    override __.Configuration = def.Configuration
                    override __.Dependencies = def.Dependencies
                    override __.PublishProtocols = def.PublishProtocols
                    override __.Behavior(conf, instanceId) = def.Behavior(conf, instanceId)
                    override __.OnDeactivate currentActivation = def.OnDeactivate currentActivation
                    override __.OnActorFailure(instanceId, conf) = def.OnActorFailure(instanceId, conf)
            }

        let rawDef = 
            {
                new RawProxyActorDefinition<'T>(def.Path, normalDef) with
                    override __.PublishProtocols = def.PublishProtocols
                    override __.OnActorFailure(instanceId, conf) = def.OnActorFailure(instanceId, conf)
            }

        [normalDef; rawDef]
