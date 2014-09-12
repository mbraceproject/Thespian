namespace Nessos.Thespian.Cluster

open System
open Nessos.Thespian
open Nessos.Thespian.AsyncExtensions
open Nessos.Thespian.Remote
open Nessos.Thespian.PowerPack
open Nessos.Thespian.Cluster.BehaviorExtensions
open Nessos.Thespian.Cluster.ActorExtensions

type private LogLevel = Nessos.Thespian.LogLevel

[<Struct; CustomComparison; CustomEquality>]
type StateGenerationNumber =
    val private generation: int

    private new (generationNum: int) = { generation = generationNum }
    static member Initial = new StateGenerationNumber(1)

    member gen.Increment() =
        if gen.generation = Int32.MaxValue then new StateGenerationNumber(0)
        else new StateGenerationNumber(gen.generation + 1)

    override gen.ToString() = gen.generation.ToString()

    member private gen.Compare(o: obj) =
        match o with
        | :? StateGenerationNumber as gen' ->
            if gen.generation <> 0 && gen'.generation <> 0 then
                gen.generation.CompareTo gen'.generation
            else if gen.generation = 0 then -1
            else 1
        | _ -> invalidArg "o" "Cannot compare objects of different types."

    override gen.Equals o =
        match o with
        | :? StateGenerationNumber as gen' -> gen.generation = gen'.generation
        | _ -> false

    override gen.GetHashCode() = hash gen.generation

    interface IComparable with
        member gen.CompareTo o = gen.Compare o

type ReplicatedState<'S> = {
    Generation: StateGenerationNumber
    State: 'S
}

module ReplicatedState =
    let create s = { Generation = StateGenerationNumber.Initial; State = s }

    let update (rs: ReplicatedState<'S>) (state: 'S) =
        { Generation = rs.Generation.Increment(); State = state }

    let same (rs: ReplicatedState<'S>) = rs


type Stateful<'S> =
    //generation, state = GetState(replyChannel)
    | GetState of IReplyChannel<ReplicatedState<'S>>
    //SetState(generation, state)
    | SetState of ReplicatedState<'S>
    //generation = GetGeneration(replyChannel)
    | GetGeneration of IReplyChannel<StateGenerationNumber * ActorRef>

[<AutoOpen>]
module StatefulUtils =
    let (|StatefulReplyException|) ctx =
        function GetState(RR ctx reply) -> reply << Exception
                 | SetState _ -> fun e -> ctx.LogEvent(LogLevel.Error, sprintf "SetState Exception:: %A" e)
                 | GetGeneration(RR ctx reply) -> reply << Exception

type Replicated<'T, 'S> =
    //Throws
    //MessageHandlingException BroadcastFailureException => failed to do any kind of replication
    | Replicated of Rely<Choice<'T, Stateful<'S>>>
    | Singular of Choice<'T, Stateful<'S>>
    | ResetReplicants of ActorRef<Choice<'T, Stateful<'S>>> list

type AsyncReplicated<'T, 'S> =
    | AsyncReplicated of Choice<'T, Stateful<'S>>
    | SyncReplicated of Rely<Choice<'T, Stateful<'S>>>
    | AsyncSingular of Choice<'T, Stateful<'S>>
    | AsyncResetReplicants of IReplyChannel<unit> * ActorRef<Choice<'T, Stateful<'S>>> list

type ReplicatedProxy<'T, 'S> =
    | ReplicatedProxy of Replicated<'T, 'S>
    | SwitchReplicated of ActorRef<Replicated<'T, 'S>>

type AsyncReplicatedProxy<'T, 'S> =
    | AsyncRepicatedProxy of AsyncReplicated<'T, 'S>
    | SwitchAsyncReplicated of ActorRef<AsyncReplicated<'T, 'S>>

module Actor =
    let private replicate (ctx: BehaviorContext<_>) reply payload actor replicants =
        async {
            try
                do! replicants
                    |> Broadcast.post payload
                    |> Broadcast.exec
                    |> Async.Ignore

                reply nothing

                actor <-- payload
            with (BroadcastPartialFailureException _) as e ->
                    ctx.LogWarning e
                    reply nothing
                    actor <-- payload
                | (BroadcastFailureException _) as e ->
                    reply <| Exception e
        }

    let private singular (ctx: BehaviorContext<_>) payload actor = 
        try
            actor <-- payload
        with e -> ctx.LogError e

    let private resetReplicants (ctx: BehaviorContext<_>) replicants replicants' actor =
        async {
            ctx.LogInfo "RESETTING replicas..."

            let! currentState = actor <!- fun ch -> Choice2Of2(GetState ch)

            let newReplicants = replicants'
            
            ctx.LogInfo <| sprintf "Old replicas are: %A" (replicants |> Seq.map (ActorRef.toUniTcpAddress >> Option.get))
            ctx.LogInfo <| sprintf "New replicas are: %A" (newReplicants |> Seq.map (ActorRef.toUniTcpAddress >> Option.get))

            for newReplicant in newReplicants do
                try
                    ReliableActorRef.FromRef newReplicant <-- Choice2Of2(SetState currentState)
                with (FailureException _) as e -> ctx.LogWarning e

            return ()
        }

    let replicatedBehavior (local: ActorRef<Choice<'T, Stateful<'S>>>) 
                           (ctx: BehaviorContext<_>) 
                           (replicants: ReliableActorRef<Choice<'T, Stateful<'S>>> list) 
                           (msg: Replicated<'T, 'S>) =
        async {
            match msg with
            | Replicated(RR ctx reply, payload) ->
                do! replicate ctx reply payload local replicants

                return replicants
            | Singular payload ->
                singular ctx payload local

                return replicants
            | ResetReplicants(replicants') ->
                do! resetReplicants ctx replicants replicants' local

                return replicants' |> List.map ReliableActorRef.FromRef
        }

    let asyncReplicatedBehavior (local: ActorRef<Choice<'T, Stateful<'S>>>)
                                (ctx: BehaviorContext<_>)
                                (replicants: ReliableActorRef<Choice<'T, Stateful<'S>>> list)
                                (msg: AsyncReplicated<'T, 'S>) =
        async {
            match msg with
            | AsyncReplicated payload ->
                try
                    do! replicants
                        |> Broadcast.post payload
                        |> Broadcast.exec
                        |> Async.Ignore

                    local <-- payload
                with (BroadcastPartialFailureException _) as e ->
                        ctx.LogWarning e
                        local <-- payload
                    | (BroadcastFailureException _) as e ->
                        ctx.LogError e

                return replicants
            | SyncReplicated(RR ctx reply, payload) ->
                do! replicate ctx reply payload local replicants

                return replicants

            | AsyncSingular payload ->
                singular ctx payload local

                return replicants
            | AsyncResetReplicants(RR ctx reply, replicants') ->
                do! resetReplicants ctx replicants replicants' local

                reply nothing

                return replicants' |> List.map ReliableActorRef.FromRef
        }
                                

    let replicated (replicants: #seq<ActorRef<Choice<'T, Stateful<'S>>>>) (actor: ActorRef<Choice<'T, Stateful<'S>>>) =
        Actor.bind <| Behavior.stateful (replicants |> Seq.map ReliableActorRef.FromRef |> Seq.toList) (replicatedBehavior actor)

    let asyncReplicated (replicants: #seq<ActorRef<Choice<'T, Stateful<'S>>>>) (actor: ActorRef<Choice<'T, Stateful<'S>>>) =
        Actor.bind <| Behavior.stateful (replicants |> Seq.map ReliableActorRef.FromRef |> Seq.toList) (asyncReplicatedBehavior actor)

    type ReplicatedProxyState<'T, 'S> = {
        Replicated: ReliableActorRef<Replicated<'T, 'S>>
        Buffer: Replicated<'T, 'S> list
    }

    type AsyncReplicatedProxyState<'T, 'S> = {
        AsyncReplicated: ReliableActorRef<AsyncReplicated<'T, 'S>>
        Buffer: AsyncReplicated<'T, 'S> list
    }

    let private (|ReplicatedExceptionReply|) ctx stateExceptionReplyExtract =
        function Replicated(RR ctx reply, _) -> reply << Exception
                 | Singular(Choice1Of2 payload) -> stateExceptionReplyExtract ctx payload
                 | Singular(Choice2Of2 (StatefulReplyException ctx reply)) -> reply
                 | ResetReplicants _ -> fun e -> ctx.LogEvent(LogLevel.Error, sprintf "ResetReplicants Exception :: %A" e)

    let private (|AsyncReplicatedExceptionReply|) ctx stateExceptionReplyExtract = 
        function AsyncReplicated(Choice1Of2 payload) -> stateExceptionReplyExtract ctx payload
                 | AsyncReplicated(Choice2Of2 (StatefulReplyException ctx reply)) -> reply
                 | SyncReplicated(RR ctx reply, _) -> reply << Exception
                 | AsyncSingular(Choice1Of2 payload) -> stateExceptionReplyExtract ctx payload
                 | AsyncSingular(Choice2Of2 (StatefulReplyException ctx reply)) -> reply
                 | AsyncResetReplicants(RR ctx reply, _) -> reply << Exception


    let replicatedProxyBehavior stateExceptionReplyExtract (ctx: BehaviorContext<_>) (state: ReplicatedProxyState<'T, 'S>) (msg: ReplicatedProxy<'T, 'S>) = async {
        match msg with
        | ReplicatedProxy(ReplicatedExceptionReply ctx stateExceptionReplyExtract exceptionReply as payload) ->
            try
                do! state.Replicated <-!- payload

                return state
            with FailureException _ as e ->
                    ctx.LogWarning e
                
                    return { state with Buffer = payload::state.Buffer }
                | e ->
                    exceptionReply e

                    return state
        | SwitchReplicated replicated ->
            ctx.LogInfo <| sprintf "Switching replicated actor reference to: %A..." replicated

            ctx.LogInfo "Emptying buffered requests..."

            let replicated' = ReliableActorRef.FromRef replicated

            for request in List.rev state.Buffer do
                do! replicated' <-!- request

            return { state with Replicated = replicated' }
    }

    let asyncReplicatedProxyBehavior stateExceptionReplyExtract (ctx: BehaviorContext<_>) (state: AsyncReplicatedProxyState<'T, 'S>) (msg: AsyncReplicatedProxy<'T, 'S>) = async {
        match msg with
        | AsyncRepicatedProxy(AsyncReplicatedExceptionReply ctx stateExceptionReplyExtract exceptionReply as payload) ->
            try
                do! state.AsyncReplicated <-!- payload

                return state
            with FailureException _ as e ->
                    ctx.LogWarning e

                    return { state with Buffer = payload::state.Buffer }
                | e ->
                    exceptionReply e

                    return state
        | SwitchAsyncReplicated asyncReplicated ->
            ctx.LogInfo <| sprintf "Switching async replicated actor reference to: %A..." asyncReplicated

            ctx.LogInfo "Emptying buffered requests..."

            let asyncReplicated' = ReliableActorRef.FromRef asyncReplicated

            for request in List.rev state.Buffer do
                do! asyncReplicated' <-!- request

            return { state with AsyncReplicated = asyncReplicated' }
    }

module AsyncReplicated =
    let constructLocalBehavior (def: Definition) 
                               (replicas: seq<ActorRef<Choice<'T, Stateful<'S>>>>) 
                               (stateMapF: 'S -> 'U)
                               (behavior: BehaviorContext<Choice<'T, Stateful<'S>>> -> 'U -> Choice<'T, Stateful<'S>> -> Async<'U>) = async {           
        def.LogInfo "Verifying replicated state consistency..."

        let! generations =
            replicas
            |> Seq.map ReliableActorRef.FromRef
            |> Broadcast.postWithReply (Choice2Of2 << GetGeneration)
            |> Broadcast.ignoreFaults Broadcast.allFaults
            |> Broadcast.exec

        def.LogInfo <| sprintf "Replicated state generations: %A" 
            (generations |> Seq.map (fun (gen, replica) -> 
                replica :?> ActorRef<Choice<'T, Stateful<'S>>> |> ActorRef.toUniTcpAddress |> Option.get, gen) |> Seq.toList)

        //NOTE!!! change this to a weaker exception that does not cause entire system fault
        if generations.Length = 0 then return! Async.Raise (SystemFailureException "Unable to obtain replicated state. Total replication failure.")

        let generationNums = generations |> Seq.map fst |> Seq.cache
        let generationNumSet = generationNums |> Set.ofSeq

        //State replicas are consistent with each other when they have the same generation number.
        //Valid incosistencies may apply in the following cases:
        //a). The generation numbers differ by one increment.
        //This applies when the replication broadcast was not able to complete due to fault.
        //Thus, some nodes have replicated the latest state while the rest have remained on the
        //immediately previous version of the state.
        //b). Some replicas are in the initial generation number, at least one is not and for
        //those with generation number <> from the initial generation number a) applies.
        //This situation arises when a new state replica has been created by recovery procedures
        //prior to the current recovery procedure.
        let isConsistent = generationNumSet |> Set.count = 1

        if isConsistent then
            def.LogInfo "Replicated state consistency verified."
            let! replicatedState =
                //FaultPoint
                //FailoverFailureException => all replicas have failed;; allow exception to propagate;; all replication lost
                replicas |> Failover.postWithReply (fun ch -> Choice2Of2(GetState ch))                    

            return Behavior.stateful (stateMapF replicatedState.State) behavior
        else
            def.LogInfo "Replicated state inconsistency detected."

            let generationNumSetModuloInitial = generationNumSet |> Set.remove StateGenerationNumber.Initial

            let isGenCountValid = 
                let count = Set.count generationNumSetModuloInitial
                count = 1 || count = 2
            if isGenCountValid then
                def.LogInfo "Repairing replicated state inconsistency..."

                let gens = generationNumSetModuloInitial |> Set.toSeq |> Seq.sort |> Seq.toArray
                                            
                let isGenDiffValid, newGen =
                    if gens.Length > 1 then
                        let oldGen = gens.[0]
                        let newGen = gens.[1]
                        oldGen.Increment() = newGen, newGen
                    else true, gens.[0]
                                            
                if isGenDiffValid then
                    let newGenReplicas = 
                        generations 
                        |> Seq.filter (fun (gen, _) -> gen = newGen) 
                        |> Seq.map snd 
                        |> Seq.cast<ActorRef<Choice<'T, Stateful<'S>>>>

                    let oldGenReplicas =
                        generations
                        |> Seq.filter (fun (gen, _) -> gen <> newGen)
                        |> Seq.map snd
                        |> Seq.cast<ActorRef<Choice<'T, Stateful<'S>>>>

                    let! state = newGenReplicas |> Failover.postWithReply (fun ch -> Choice2Of2(GetState ch))

                    do! oldGenReplicas
                        |> Seq.map ReliableActorRef.FromRef
                        |> Broadcast.post (Choice2Of2(SetState state))
                        |> Broadcast.ignoreFaults Broadcast.allFaults
                        |> Broadcast.exec
                        |> Async.Ignore

                    def.LogInfo "Replicated state consistency recovered."

                    return Behavior.stateful (stateMapF state.State) behavior
                else
                    return! Async.Raise (SystemFailureException "Replicated state inconsistency unrecoverable.")
            else
                //NOTE!!! change this to a weaker exception that does not cause entire system fault
                return! Async.Raise (SystemFailureException "Unable to obtain replicated state. State generation difference is greater than 1.")
    }

type ReplicatedActorSpec = {
    AddRaw: bool
    Publish: IProtocolFactory list
} with static member Default = { AddRaw = false; Publish = [Protocols.utcp()] }

type ReplicatedActorDefinitionConfiguration = {
    Replicating: ReplicatedActorSpec
    Local: ReplicatedActorSpec
    Replicated: ReplicatedActorSpec
    AsyncReplication: bool
} with
    static member Default = { 
        Replicating = ReplicatedActorSpec.Default 
        Local = ReplicatedActorSpec.Default
        Replicated = ReplicatedActorSpec.Default 
        AsyncReplication = false
    }

[<AbstractClass>]
type ReplicatedActorDefinition<'T, 'U, 'S>(parent: DefinitionPath, stateMapF: 'S -> 'U, definitionConfig: ReplicatedActorDefinitionConfiguration) =
    inherit GroupDefinition(parent)

    abstract ReplicaOnNodeLossAction: NodeLossAction
    default __.ReplicaOnNodeLossAction = ReActivateOnNodeLoss

    abstract ReplicatedOnNodeLossAction: NodeLossAction
    default __.ReplicatedOnNodeLossAction = DoNothingOnNodeLoss

    member private self.ReplicatingDependecncy (conf: ActivationConfiguration) =
        let replicationFactor = conf.Get<int>(Configuration.ReplicationFactor)

        let dependencyPath =
            if definitionConfig.Replicating.AddRaw then self.Path/"replica"/(self.Name + "Raw")
            else self.Path/"replica"/self.Name

        {
            Definition = dependencyPath
            Instance = None //same instance id as this group
            Configuration = self.Configuration
            ActivationStrategy = ActivationStrategy.selectNodes replicationFactor
        }

    member private self.Replicating =
        {
            new GroupDefinition(self.Path) with
                override __.Name = "replica"
                override def.Collocated = 
                    let replicaDef =
                        {
                            new ActorDefinition<Choice<'T, Stateful<'S>>>(def.Path) with
                                override __.Name = self.Name
                                override __.Configuration = self.Configuration
                                override __.Dependencies = []
                                override __.PublishProtocols =
                                    if definitionConfig.Replicating.Publish.IsEmpty then invalidArg "definitionConfig" "Replicating definition is not allowed to be local."
                                    definitionConfig.Replicating.Publish
                                override __.Behavior(conf, instanceId) = async {
                                    return self.Behavior(conf, instanceId, self.InitState)
                                }

                                override __.OnNodeLossAction _ = self.ReplicaOnNodeLossAction
                         }

                    let replicaRawDef =
                        {
                            new RawProxyActorDefinition<Choice<'T, Stateful<'S>>>(def.Path, replicaDef) with
                                override __.PublishProtocols =
                                    if definitionConfig.Replicating.Publish.IsEmpty then invalidArg "definitionConfig" "Replicating definition is not allowed to be local."
                                    definitionConfig.Replicating.Publish

                                override __.OnNodeLossAction _ = self.ReplicaOnNodeLossAction
                        } :> Definition
                    
                    replicaDef :> Definition
                    ::
                    if definitionConfig.Replicating.AddRaw then replicaRawDef::[] else []
        }

    member private self.Local =
        {
            new GroupDefinition(self.Path) with
                override __.Name = "local"
                override def.Collocated =
                    let localDef =
                        {
                            new ActorDefinition<Choice<'T, Stateful<'S>>>(def.Path) with
                                override __.Name = self.Name
                                override __.Configuration = self.Configuration
                                override __.Dependencies = [self.ReplicatingDependecncy self.Configuration]
                                override __.GetDependencies(conf') = [self.ReplicatingDependecncy (self.Configuration.Override conf')]
                                override __.PublishProtocols = definitionConfig.Local.Publish
                                override def'.Behavior(conf, instanceId) = async {
                                    let replicas =
                                        if definitionConfig.Replicating.AddRaw then
                                            let path = self.Path/"replica"/(self.Name + "Raw")
                                            let rawReplicas = Cluster.NodeRegistry.Resolve<RawProxy> { Definition = path; InstanceId = instanceId }
                                            rawReplicas |> Seq.map (fun rawProxyRef -> new RawActorRef<Choice<'T, Stateful<'S>>>(rawProxyRef) :> ActorRef<_>)
                                        else
                                            let path = self.Path/"replica"/self.Name
                                            NodeRegistry.Registry.Resolve<Choice<'T, Stateful<'S>>> { Definition = path; InstanceId = instanceId }

                                    def'.LogInfo "Verifying replicated state consistency..."

                                    let! generations =
                                        replicas
                                        |> Seq.map ReliableActorRef.FromRef
                                        |> Broadcast.postWithReply (Choice2Of2 << GetGeneration)
                                        |> Broadcast.ignoreFaults Broadcast.allFaults
                                        |> Broadcast.exec

                                    def'.LogInfo <| sprintf "Replicated state generations: %A" 
                                        (generations |> Seq.map (fun (gen, replica) -> 
                                            replica :?> ActorRef<Choice<'T, Stateful<'S>>> |> ActorRef.toUniTcpAddress |> Option.get, gen) |> Seq.toList)

                                    //NOTE!!! change this to a weaker exception that does not cause entire system fault
                                    if generations.Length = 0 then return! Async.Raise (SystemFailureException "Unable to obtain replicated state. Total replication failure.")

                                    let generationNums = generations |> Seq.map fst |> Seq.cache
                                    let generationNumSet = generationNums |> Set.ofSeq

                                    //State replicas are consistent with each other when they have the same generation number.
                                    //Valid incosistencies may apply in the following cases:
                                    //a). The generation numbers differ by one increment.
                                    //This applies when the replication broadcast was not able to complete due to fault.
                                    //Thus, some nodes have replicated the latest state while the rest have remained on the
                                    //immediately previous version of the state.
                                    //b). Some replicas are in the initial generation number, at least one is not and for
                                    //those with generation number <> from the initial generation number a) applies.
                                    //This situation arises when a new state replica has been created by recovery procedures
                                    //prior to the current recovery procedure.
                                    let isConsistent = generationNumSet |> Set.count = 1

                                    if isConsistent then
                                        def'.LogInfo "Replicated state consistency verified."
                                        let! replicatedState =
                                            //FaultPoint
                                            //FailoverFailureException => all replicas have failed;; allow exception to propagate;; all replication lost
                                            replicas |> Failover.postWithReply (fun ch -> Choice2Of2(GetState ch))                    

                                        return self.Behavior(conf, instanceId, stateMapF replicatedState.State)
                                    else
                                        def'.LogInfo "Replicated state inconsistency detected."

                                        let generationNumSetModuloInitial = generationNumSet |> Set.remove StateGenerationNumber.Initial

                                        let isGenCountValid = 
                                            let count = Set.count generationNumSetModuloInitial
                                            count = 1 || count = 2
                                        if isGenCountValid then
                                            def'.LogInfo "Repairing replicated state inconsistency..."

                                            let gens = generationNumSetModuloInitial |> Set.toSeq |> Seq.sort |> Seq.toArray
                                            
                                            let isGenDiffValid, newGen =
                                                if gens.Length > 1 then
                                                    let oldGen = gens.[0]
                                                    let newGen = gens.[1]
                                                    oldGen.Increment() = newGen, newGen
                                                else true, gens.[0]
                                            
                                            if isGenDiffValid then
                                                let newGenReplicas = 
                                                    generations 
                                                    |> Seq.filter (fun (gen, _) -> gen = newGen) 
                                                    |> Seq.map snd 
                                                    |> Seq.cast<ActorRef<Choice<'T, Stateful<'S>>>>

                                                let oldGenReplicas =
                                                    generations
                                                    |> Seq.filter (fun (gen, _) -> gen <> newGen)
                                                    |> Seq.map snd
                                                    |> Seq.cast<ActorRef<Choice<'T, Stateful<'S>>>>

                                                let! state = newGenReplicas |> Failover.postWithReply (fun ch -> Choice2Of2(GetState ch))

                                                do! oldGenReplicas
                                                    |> Seq.map ReliableActorRef.FromRef
                                                    |> Broadcast.post (Choice2Of2(SetState state))
                                                    |> Broadcast.ignoreFaults Broadcast.allFaults
                                                    |> Broadcast.exec
                                                    |> Async.Ignore

                                                def'.LogInfo "Replicated state consistency recovered."

                                                return self.Behavior(conf, instanceId, stateMapF state.State)
                                            else
                                                return! Async.Raise (SystemFailureException "Replicated state inconsistency unrecoverable.")
                                        else
                                            //NOTE!!! change this to a weaker exception that does not cause entire system fault
                                            return! Async.Raise (SystemFailureException "Unable to obtain replicated state. State generation difference is greater than 1.")
                                }
                        }

                    let localRawDef =
                        new RawProxyActorDefinition<Choice<'T, Stateful<'S>>>(def.Path, localDef) :> Definition

                    localDef :> Definition
                    ::
                    if definitionConfig.Local.Publish.IsEmpty && definitionConfig.Local.AddRaw then invalidArg "definitionConfig" "Local in ReplicatedActorDefinition cannot be both unpublished and raw."
                    elif definitionConfig.Local.AddRaw then localRawDef::[]
                    else []
        }

    member private self.Replicated =
        let getReplicas instanceId =
            if definitionConfig.Replicating.AddRaw then
                Cluster.NodeRegistry.Resolve { Definition = self.Path/"replica"/(self.Name + "Raw"); InstanceId = instanceId } 
                |> Seq.map RawActorRef.FromRawProxy
            else 
                Cluster.NodeRegistry.Resolve { Definition = self.Path/"replica"/self.Name; InstanceId = instanceId }

        let constructBehavior behaviorConstructor instanceId =
            let replicas = getReplicas instanceId

            //local is collocacted with replicated
            //even if local is set to have a raw, we not need use it here
            let local = Cluster.NodeRegistry.ResolveLocal<Choice<'T, Stateful<'S>>>
                            { Definition = self.Path/"local"/self.Name; InstanceId = instanceId }
//                            (if definitionConfig.Local.AddRaw then { Definition = self.Path/"local"/(self.Name + "Raw"); InstanceId = instanceId }
//                             else { Definition = self.Path/"local"/self.Name; InstanceId = instanceId })


            Behavior.stateful (replicas |> Seq.map ReliableActorRef.FromRef |> Seq.toList) (behaviorConstructor local)

//        let dependency (conf: ActivationConfiguration) =
//            let replicationFactor = conf.Get<int>(Configuration.ReplicationFactor)
//
//            let dependencyPath =
//                if definitionConfig.Replicating.AddRaw then self.Path/"replica"/(self.Name + "Raw")
//                else self.Path/"replica"/self.Name
//
//            {
//                Definition = dependencyPath
//                Instance = None //same instance id as this group
//                Configuration = self.Configuration
//                ActivationStrategy = ActivationStrategy.selectNodes replicationFactor
//            }

        let localDependency = {
            Definition = self.Path/"local"/self.Name
            Instance = None
            Configuration = self.Configuration
            ActivationStrategy = ActivationStrategy.collocated
        }

        {
            new GroupDefinition(self.Path) with
                override __.Name = "replicated"
                override def.Collocated =
                    let replicatedDef, replicatedRawDef =
                        if definitionConfig.AsyncReplication then
                            let replicatedDef =
                                {
                                    new ActorDefinition<AsyncReplicated<'T, 'S>>(def.Path) with
                                        override __.Name = self.Name
                                        override __.Configuration = self.Configuration
                                        override __.Dependencies = [localDependency; self.ReplicatingDependecncy self.Configuration]
                                        override __.GetDependencies(conf') = [{ localDependency with Configuration = localDependency.Configuration.Override conf' }; self.ReplicatingDependecncy(self.Configuration.Override conf')]
                                        override __.PublishProtocols = definitionConfig.Replicated.Publish
                                        override __.Behavior(_, instanceId) = async {
                                            return constructBehavior Actor.asyncReplicatedBehavior instanceId
                                        }
                                        override def.OnDependencyLossRecovery(currentActivation: Activation, lostDependency: ActivationReference) = async {
                                            def.LogInfo "Resetting replicas..."
                                            let replicated = currentActivation.Resolve<AsyncReplicated<'T, 'S>>
                                                                { Definition = def.Path; InstanceId = currentActivation.InstanceId }

                                            let replicas = getReplicas currentActivation.InstanceId |> Seq.toList

                                            def.LogInfo <| sprintf "New replicas: %A" (replicas |> List.map (ActorRef.toUniTcpAddress >> Option.get))

                                            do! replicated <!- fun ch -> AsyncResetReplicants(ch, replicas)

                                            def.LogInfo "Replices reset."
                                        }

                                        override __.OnNodeLossAction _ = self.ReplicatedOnNodeLossAction
                                }

                            let replicatedRawDef =
                                new RawProxyActorDefinition<AsyncReplicated<'T, 'S>>(def.Path, replicatedDef)

                            replicatedDef :> Definition, replicatedRawDef :> Definition
                        else
                            let replicatedDef =
                                {
                                    new ActorDefinition<Replicated<'T, 'S>>(def.Path) with
                                        override __.Name = self.Name
                                        override __.Configuration = self.Configuration
                                        override __.Dependencies = [localDependency; self.ReplicatingDependecncy self.Configuration]
                                        override __.GetDependencies(conf') = [{ localDependency with Configuration = localDependency.Configuration.Override conf' }; self.ReplicatingDependecncy (self.Configuration.Override conf')]
                                        override __.PublishProtocols = definitionConfig.Replicated.Publish
                                        override __.Behavior(_, instanceId) = async {
                                            return constructBehavior Actor.replicatedBehavior instanceId
                                        }

                                        override def.OnDependencyLossRecovery(currentActivation: Activation, lostDependency: ActivationReference) = async {
                                            def.LogInfo "Resetting replicas..."
                                            let replicated = currentActivation.Resolve<Replicated<'T, 'S>>
                                                                { Definition = def.Path; InstanceId = currentActivation.InstanceId }

                                            let replicas = getReplicas currentActivation.InstanceId |> Seq.toList
                                            
                                            def.LogInfo <| sprintf "New replicas: %A" (replicas |> List.map (ActorRef.toUniTcpAddress >> Option.get))

                                            replicated <-- ResetReplicants replicas
                                        }

                                        override __.OnNodeLossAction _ = self.ReplicatedOnNodeLossAction
                                }
                            
                            let replicatedRawDef =
                                new RawProxyActorDefinition<Replicated<'T, 'S>>(def.Path, replicatedDef)

                            replicatedDef :> Definition, replicatedRawDef :> Definition

                    replicatedDef
                    ::
                    if definitionConfig.Replicated.Publish.IsEmpty && definitionConfig.Replicated.AddRaw then invalidArg "definitionConfig" "Replicated in ReplicatedActorDefinition cannot be both unpublished and raw."
                    elif definitionConfig.Replicated.AddRaw then replicatedRawDef::[]
                    else []
        }

    new (parent: DefinitionPath, stateMapF: 'S -> 'U) = ReplicatedActorDefinition<'T, 'U, 'S>(parent, stateMapF, ReplicatedActorDefinitionConfiguration.Default)

    override __.Configuration =
        ActivationConfiguration.Specify 
            [Conf.Spec(Configuration.ReplicationFactor, 3)]

    abstract InitState: 'U
    abstract Behavior: ActivationConfiguration * int * 'U -> (Actor<Choice<'T, Stateful<'S>>> -> Async<unit>)

    override def.Collocated = [def.Local; def.Replicated]
    override def.Components = (def.Replicating :> Definition)::def.Collocated


[<AbstractClass>]
type ReplicatedActorDefinition<'T, 'S>(parent: DefinitionPath, definitionConfiguration: ReplicatedActorDefinitionConfiguration) =
    inherit ReplicatedActorDefinition<'T, 'S, 'S>(parent, id, definitionConfiguration)

    new (parent: DefinitionPath) = ReplicatedActorDefinition<'T, 'S>(parent, ReplicatedActorDefinitionConfiguration.Default)


[<AbstractClass>]
type ReplicatedProxyActorDefinition<'T, 'S>(parent: DefinitionPath, replicatedDependency: ActivationRecord) =
    inherit ActorDefinition<ReplicatedProxy<'T, 'S>>(parent)

    abstract ExceptionReplyProvider: unit -> (BehaviorContext<ReplicatedProxy<'T, 'S>> -> 'T -> exn -> unit) with get

    override __.Dependencies = [replicatedDependency]

    override def.Behavior(configuration: ActivationConfiguration, instanceId: int) = async {
        let instanceId' = replicatedDependency.Instance |> Option.bind2 id instanceId
        let replicated = Cluster.NodeRegistry.Resolve<Replicated<'T, 'S>> { Definition = replicatedDependency.Definition; InstanceId = instanceId' }
                         |> Seq.head

        let state = { Replicated = ReliableActorRef.FromRef replicated; Buffer = [] } : Actor.ReplicatedProxyState<'T, 'S>

        return Behavior.stateful state (Actor.replicatedProxyBehavior def.ExceptionReplyProvider)
    }

    override def.OnDependencyLossRecovery(dependantActivation, dependency) = 
        let baseOnDependencyLossRecovery = base.OnDependencyLossRecovery(dependantActivation, dependency)
        async {
            do! baseOnDependencyLossRecovery

            if dependency.Definition = replicatedDependency.Definition then
                def.LogInfo <| sprintf "Dependecny %O of dependant %O recovered." dependantActivation.ActivationReference dependency
                let replicated = Cluster.NodeRegistry.Resolve<Replicated<'T, 'S>> dependency |> Seq.head

                let replicatedProxy = dependantActivation.Resolve<ReplicatedProxy<'T, 'S>> dependantActivation.ActivationReference

                def.LogInfo <| sprintf "Switching replicated actor ref with %A..." replicated

                do! replicatedProxy <-!- SwitchReplicated replicated
        }


[<AbstractClass>]
type AsyncReplicatedProxyActorDefinition<'T, 'S>(parent: DefinitionPath, replicatedDependency: ActivationRecord) =
    inherit ActorDefinition<AsyncReplicatedProxy<'T, 'S>>(parent)

    abstract ExceptionReplyProvider: BehaviorContext<AsyncReplicatedProxy<'T, 'S>> -> 'T -> exn -> unit

    override __.Dependencies = [replicatedDependency]

    override def.Behavior(configuration: ActivationConfiguration, instanceId: int) = async {
        let replicated = Cluster.NodeRegistry.Resolve<AsyncReplicated<'T, 'S>> { Definition = replicatedDependency.Definition; InstanceId = instanceId }
                         |> Seq.head

        let state = { AsyncReplicated = ReliableActorRef.FromRef replicated; Buffer = [] } : Actor.AsyncReplicatedProxyState<'T, 'S>

        return Behavior.stateful state (Actor.asyncReplicatedProxyBehavior def.ExceptionReplyProvider)
    }
        

