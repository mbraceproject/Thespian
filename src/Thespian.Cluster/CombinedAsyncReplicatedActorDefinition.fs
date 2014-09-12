namespace Nessos.Thespian.Cluster

open Nessos.Thespian
open Nessos.Thespian.AsyncExtensions
open Nessos.Thespian.Remote
open Nessos.Thespian.Cluster.BehaviorExtensions
open Nessos.Thespian.Cluster.ActorExtensions

type AsyncReplicatedDirection = Left | Right

type CombinedAsyncReplicated<'T1, 'S1, 'T2, 'S2> =
    | FwdLeft of AsyncReplicated<'T1, 'S1>
    | FwdRight of AsyncReplicated<'T2, 'S2>
    | SuspendReplication of AsyncReplicatedDirection
    | ResumeReplication of AsyncReplicatedDirection

module CombinedAsyncReplicated =

    type Status = Normal | Suspended

    type State<'T1, 'S1, 'T2, 'S2> = {
        StatusLeft: Status
        StatusRight: Status
        InnerStateLeft: ReliableActorRef<Choice<'T1, Stateful<'S1>>> list
        InnerStateRight: ReliableActorRef<Choice<'T2, Stateful<'S2>>> list
        SuspendedBuffer: CombinedAsyncReplicated<'T1, 'S1, 'T2, 'S2> list
    } with
        static member Init(leftReplicas, rightReplicas) = {
            StatusLeft = Normal
            StatusRight = Normal
            InnerStateLeft = leftReplicas
            InnerStateRight = rightReplicas
            SuspendedBuffer = []
        }

    let rec behavior (behaviorLeft: BehaviorContext<_> -> ReliableActorRef<Choice<'T1, Stateful<'S1>>> list -> AsyncReplicated<'T1, 'S1> -> Async<ReliableActorRef<Choice<'T1, Stateful<'S1>>> list>)
                     (behaviorRight: BehaviorContext<_> -> ReliableActorRef<Choice<'T2, Stateful<'S2>>> list -> AsyncReplicated<'T2, 'S2> -> Async<ReliableActorRef<Choice<'T2, Stateful<'S2>>> list>)
                     (ctx: BehaviorContext<_>) 
                     (state: State<'T1, 'S1, 'T2, 'S2>) 
                     (msg: CombinedAsyncReplicated<'T1, 'S1, 'T2, 'S2>) = 
        let leftCtx = ctx |> BehaviorContext.map FwdLeft
        let rightCtx = ctx |> BehaviorContext.map FwdRight
        async {
            match state.StatusLeft, state.StatusRight, msg with
            | Suspended, _, SuspendReplication Right
            | _, Suspended, SuspendReplication Left ->
                ctx.LogWarning "Replication is already suspsended."
                return state
            | Suspended, Suspended, ResumeReplication Left ->
                ctx.LogInfo "Resuming replication (left)..."
                return { state with StatusLeft = Normal }
            | Suspended, Suspended, ResumeReplication Right ->
                ctx.LogInfo "Resuming replication (right)..."
                return { state with StatusRight = Normal }
            | Normal, Suspended, ResumeReplication Right
            | Suspended, Normal, ResumeReplication Left ->
                ctx.LogInfo "Resuming replication (both)..."

                let state' = { state with StatusLeft = Normal; StatusRight = Normal; SuspendedBuffer = [] }

                let suspendedMessages = List.rev state.SuspendedBuffer

                ctx.LogInfo "Processing suspended messages..."

                let! state'' = state' |> List.foldBackAsync (fun m s -> behavior behaviorLeft behaviorRight ctx s m) suspendedMessages

                ctx.LogInfo "Suspended messages processed. Replication is resumed."

                return state''
            | _, _, FwdLeft(AsyncResetReplicants _ as payload) ->
                ctx.LogInfo "Resetting replicas (left)..."
                let! stateLeft = behaviorLeft leftCtx state.InnerStateLeft payload

                return { state with InnerStateLeft = stateLeft }
            | _, _, FwdRight(AsyncResetReplicants _ as payload) ->
                ctx.LogInfo "Resetting replicas (right)..."
                let! stateRight = behaviorRight rightCtx state.InnerStateRight payload

                return { state with InnerStateRight = stateRight }
            | Suspended, _, msg
            | _, Suspended, msg ->
                return { state with SuspendedBuffer = msg::state.SuspendedBuffer }
            | Normal, _, ResumeReplication Left
            | _, Normal, ResumeReplication Right ->
                ctx.LogWarning "Replication is active."
                return state
            | _, Normal, SuspendReplication Right ->
                ctx.LogInfo "Replication is suspended (right)."
                return { state with StatusLeft = Suspended }
            | Normal, _, SuspendReplication Left ->
                ctx.LogInfo "Replication is suspended (left)."
                return { state with StatusRight = Suspended }
            | Normal, Normal, FwdLeft payload ->
                let! stateLeft = behaviorLeft leftCtx state.InnerStateLeft payload

                return { state with InnerStateLeft = stateLeft }
            | Normal, Normal, FwdRight payload ->
                let! stateRight = behaviorRight rightCtx state.InnerStateRight payload

                return { state with InnerStateRight = stateRight }
        }

module private Defs =
    let replicaDef (path: DefinitionPath) 
                   (name: string)
                   (conf: ActivationConfiguration)
                   (initState: 'U)
                   (behavior: ActivationConfiguration * int -> (BehaviorContext<Choice<'T, Stateful<'S>>> -> 'U -> Choice<'T, Stateful<'S>> -> Async<'U>)) =
        { new ActorDefinition<Choice<'T, Stateful<'S>>>(path) with
            override __.Name = name
            override __.Configuration = conf
            override __.Dependencies = []
            override __.PublishProtocols = [Protocols.utcp()]
            override __.Behavior(conf, instanceId) = async {
                return Behavior.stateful initState (behavior(conf, instanceId))
            }
//                override __.OnNodeLossAction _ = self.ReplicaOnNodeLossAction
        }

    let replicaRawDef path replicaDef onNodeLossAction =
        {
            new RawProxyActorDefinition<Choice<'T, Stateful<'S>>>(path, replicaDef) with
                override __.PublishProtocols = [Protocols.utcp()]

                override __.OnNodeLossAction _ = onNodeLossAction
        }

    let localDef path name conf dependency getReplicas behaviorF stateMapF =
        {
            new ActorDefinition<Choice<'T, Stateful<'S>>>(path) with
                override __.Name = name
                override __.Configuration = conf
                override def.Dependencies = [dependency def.Configuration]
                override __.GetDependencies(conf') = [dependency conf']
                override __.PublishProtocols = [Protocols.utcp()]
                override def.Behavior(conf, instanceId) = async {
                    let replicas = getReplicas instanceId
                    
                    let behavior = behaviorF(conf, instanceId)

                    return! AsyncReplicated.constructLocalBehavior def replicas stateMapF behavior
                }
        }

    let localRawDef localPath localDef = new RawProxyActorDefinition<Choice<'T, Stateful<'S>>>(localPath, localDef)

type private DefsHelper<'T1, 'U1, 'S1, 'T2, 'U2, 'S2>(self: CombinedAsyncReplicatedActorDefinition<'T1, 'U1, 'S1, 'T2, 'U2, 'S2>, stateMapF1: 'S1 -> 'U1, stateMapF2: 'S2 -> 'U2) =
    let replicaName = "replica"
    let replicaPath = self.Path/replicaName
    let replicaDef name initState behavior = Defs.replicaDef replicaPath name self.Configuration initState behavior
    let replica1Def = replicaDef self.Name1 self.InitState1 self.Behavior1
    let replica2Def = replicaDef self.Name2 self.InitState2 self.Behavior2        
    let replica1RawDef = Defs.replicaRawDef replicaPath replica1Def self.ReplicaOnNodeLossAction
    let replica2RawDef = Defs.replicaRawDef replicaPath replica2Def self.ReplicaOnNodeLossAction

    let replicaDependency path conf =
        let conf' = self.Configuration.Override(conf)
        {
            Definition = path
            Instance = None
            Configuration = conf'
            ActivationStrategy = self.ReplicaActivationStrategy conf'
        }
    let replica1Dependency = replicaDependency replica1RawDef.Path
    let replica2Dependency = replicaDependency replica2RawDef.Path

    let rawReplicas path instanceId =
        Cluster.NodeRegistry.Resolve<RawProxy> { Definition = path; InstanceId = instanceId }
    let rawReplicas1 = rawReplicas replica1RawDef.Path
    let rawReplicas2 = rawReplicas replica2RawDef.Path
    let getReplicas1 instanceId =
        rawReplicas1 instanceId |> Seq.map (fun rawProxyRef -> new RawActorRef<Choice<'T1, Stateful<'S1>>>(rawProxyRef) :> ActorRef<_>)
    let getReplicas2 instanceId =
        rawReplicas2 instanceId |> Seq.map (fun rawProxyRef -> new RawActorRef<Choice<'T2, Stateful<'S2>>>(rawProxyRef) :> ActorRef<_>)
    let replicaDefinition =
        let collocated = [replica1RawDef :> Definition; replica2RawDef :> Definition]
        let components = [replica1Def :> Definition; replica2Def :> Definition]@collocated

        { new GroupDefinition(self.Path) with
            override __.Name = replicaName
            override __.Components = components
            override __.Collocated = collocated
        }


    let localName = "local"
    let localPath = self.Path/localName
    let localDef name dependencyF replicasF behaviorF stateMapF = Defs.localDef localPath name self.Configuration dependencyF replicasF behaviorF stateMapF
    let local1Def = localDef self.Name1 replica1Dependency getReplicas1 self.Behavior1 stateMapF1
    let local2Def = localDef self.Name2 replica2Dependency getReplicas2 self.Behavior2 stateMapF2
    let local1RawDef = Defs.localRawDef localPath local1Def
    let local2RawDef = Defs.localRawDef localPath local2Def

    let localDefinition =
        let collocated = [local1RawDef :> Definition; local2RawDef :> Definition]
        let components = [local1Def :> Definition; local2Def :> Definition]@collocated

        { new GroupDefinition(self.Path) with
            override __.Name = localName
            override __.Components = components
            override __.Collocated = collocated
        }


    let replicatedName = "replicated"
    let replicatedPath = self.Path/replicatedName
    let localDependency path = {
        Definition = path
        Instance = None
        Configuration = self.Configuration
        ActivationStrategy = ActivationStrategy.collocated
    }
    let local1Dependency = localDependency local1Def.Path
    let local2Dependency = localDependency local2Def.Path
    let replicatedDef =
        {
            new ActorDefinition<CombinedAsyncReplicated<'T1, 'S1, 'T2, 'S2>>(replicatedPath) with
                override __.Name = self.Name
                override __.Configuration = self.Configuration
                override __.Dependencies = [local1Dependency; local2Dependency; replica1Dependency self.Configuration; replica2Dependency self.Configuration]
                override __.GetDependencies conf' =
                    [ { local1Dependency with Configuration = local1Dependency.Configuration.Override conf' }
                      { local2Dependency with Configuration = local2Dependency.Configuration.Override conf' }
                      replica1Dependency conf'
                      replica2Dependency conf' ]
                override __.PublishProtocols = [Protocols.utcp()]
                override __.Behavior(_, instanceId) = async {
                    let replicas1 = getReplicas1 instanceId |> Seq.map ReliableActorRef.FromRef |> Seq.toList
                    let replicas2 = getReplicas2 instanceId |> Seq.map ReliableActorRef.FromRef |> Seq.toList

                    let local1 = Cluster.NodeRegistry.ResolveLocal<Choice<'T1, Stateful<'S1>>> { Definition = local1Def.Path; InstanceId = instanceId }
                    let local2 = Cluster.NodeRegistry.ResolveLocal<Choice<'T2, Stateful<'S2>>> { Definition = local2Def.Path; InstanceId = instanceId }

                    let initState = CombinedAsyncReplicated.State<'T1, 'S1, 'T2, 'S2>.Init(replicas1, replicas2)

                    let behavior = CombinedAsyncReplicated.behavior (Actor.asyncReplicatedBehavior local1) (Actor.asyncReplicatedBehavior local2)

                    return Behavior.stateful initState behavior
                }
                override def.OnDependencyLoss(currentActivation, lostDependency, lostNode) = 
                    let baseOnDependencyLoss = base.OnDependencyLoss(currentActivation, lostDependency, lostNode)
                    async {
                        do! baseOnDependencyLoss

                        let replicated = 
                            currentActivation.Resolve<CombinedAsyncReplicated<'T1, 'S1, 'T2, 'S2>> { Definition = def.Path; InstanceId = currentActivation.InstanceId }

                        if lostDependency.Definition = (replica1Dependency self.Configuration).Definition then
                            def.LogInfo "Lost replicated dependency. Suspending replication..."
                            do! replicated <-!- SuspendReplication Left
                            def.LogInfo "Triggerred replication suspension."
                        else if lostDependency.Definition = (replica2Dependency self.Configuration).Definition then
                            def.LogInfo "Lost replicated dependency. Suspending replication..."
                            do! replicated <-!- SuspendReplication Right
                            def.LogInfo "Triggerred replication suspension."
                    }
                override def.OnDependencyLossRecovery(currentActivation, lostDependency) =
                    let baseOnDependencyLossRecovery = base.OnDependencyLossRecovery(currentActivation, lostDependency)
                    async {
                        do! baseOnDependencyLossRecovery
                        
                        let replicated = 
                            currentActivation.Resolve<CombinedAsyncReplicated<'T1, 'S1, 'T2, 'S2>> { Definition = def.Path; InstanceId = currentActivation.InstanceId }

                        if lostDependency.Definition = (replica1Dependency self.Configuration).Definition then
                            def.LogInfo "Resetting replicas..." 
                            
                            let leftReplicas = getReplicas1 currentActivation.InstanceId |> Seq.toList

                            def.LogInfo <| sprintf "New replicas (left): %A" (leftReplicas |> List.map (ActorRef.toUniTcpAddress >> Option.get))

                            do! replicated <!- fun ch -> FwdLeft(AsyncResetReplicants(ch, leftReplicas))

                            def.LogInfo "Replicas have been reset. Resuming replication..."

                            do! replicated <-!- ResumeReplication Left
                        else if lostDependency.Definition = (replica2Dependency self.Configuration).Definition then
                            def.LogInfo "Resetting replicas..." 

                            let rightReplicas = getReplicas2 currentActivation.InstanceId |> Seq.toList

                            def.LogInfo <| sprintf "New replicas (right): %A" (rightReplicas |> List.map (ActorRef.toUniTcpAddress >> Option.get))
                    
                            do! replicated <!- fun ch -> FwdRight(AsyncResetReplicants(ch, rightReplicas))

                            def.LogInfo "Replicas have been reset. Resuming replication..."

                            do! replicated <-!- ResumeReplication Right
                    }
                override __.OnNodeLossAction _ = self.ReplicatedOnNodeLossAction
        }
    let replicatedRawDef = new RawProxyActorDefinition<CombinedAsyncReplicated<'T1, 'S1, 'T2, 'S2>>(replicatedPath, replicatedDef)

    let replicatedDefinition =
        let collocated = [replicatedRawDef :> Definition]
        let components = (replicatedDef :> Definition)::collocated

        { new GroupDefinition(self.Path) with
            override __.Name = replicatedName
            override __.Components = components
            override __.Collocated = collocated
        }

    let collocated = [localDefinition :> Definition; replicatedDefinition :> Definition]
    let components = (replicaDefinition :> Definition)::collocated

    member __.Collocated = collocated
    member __.Components = components

and [<AbstractClass>] CombinedAsyncReplicatedActorDefinition<'T1, 'U1, 'S1, 'T2, 'U2, 'S2>(parent: DefinitionPath, stateMapF1: 'S1 -> 'U1, stateMapF2: 'S2 -> 'U2) =
    inherit GroupDefinition(parent)

    member private self.Defs = new DefsHelper<'T1, 'U1, 'S1, 'T2, 'U2, 'S2>(self, stateMapF1, stateMapF2)

    override self.Collocated = self.Defs.Collocated
    override self.Components = self.Defs.Components
    override __.Configuration = ActivationConfiguration.Specify [Conf.Spec(Configuration.ReplicationFactor, 3)]

    abstract Name1: string
    abstract Name2: string

    abstract InitState1: 'U1
    abstract InitState2: 'U2

    abstract ReplicaOnNodeLossAction: NodeLossAction
    default __.ReplicaOnNodeLossAction = ReActivateOnNodeLoss

    abstract ReplicatedOnNodeLossAction: NodeLossAction
    default __.ReplicatedOnNodeLossAction = DoNothingOnNodeLoss

    abstract ReplicaActivationStrategy: ActivationConfiguration -> IActivationStrategy
    default __.ReplicaActivationStrategy conf = 
        let replicationFactor = conf.Get<int>(Configuration.ReplicationFactor)
        ActivationStrategy.selectNodes replicationFactor

    abstract Behavior1: ActivationConfiguration * int -> (BehaviorContext<Choice<'T1, Stateful<'S1>>> -> 'U1 -> Choice<'T1, Stateful<'S1>> -> Async<'U1>)
    abstract Behavior2: ActivationConfiguration * int -> (BehaviorContext<Choice<'T2, Stateful<'S2>>> -> 'U2 -> Choice<'T2, Stateful<'S2>> -> Async<'U2>)


