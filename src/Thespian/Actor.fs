namespace Thespian

    open System
    open Thespian.Utils

    [<AbstractClass>]
    type ActorBase() =
        abstract Log: IEvent<Log>
        abstract LogEvent: LogLevel * 'E -> unit
        abstract LogInfo: 'I -> unit
        abstract LogWarning: 'W -> unit
        abstract LogError: exn -> unit
        abstract Start: unit -> unit
        abstract Stop: unit -> unit

    type Actor<'T>(actorUUId: ActorUUID, name: string, protocols: IActorProtocol<'T>[], behavior: Actor<'T> -> Async<unit>, ?linkedActorsOpt: seq<ActorBase>) as self =
        inherit ActorBase()

        do
            if protocols = Array.empty then
                raise (new ArgumentException("No protocols specified."))

            match protocols.[0] with
            | :? IPrincipalActorProtocol<'T> -> ()
            | _ -> raise (new ArgumentException("First specified protocol is not a principal protocol."))

            if protocols |> Array.exists (fun protocol -> protocol.ActorUUId <> actorUUId) then
                raise (new ArgumentException("ActorId mismatch in protocols."))

        let mutable currentBehavior = behavior

        let mutable isStarted = false
        
        let mutable linkedErrorRemoves: IDisposable list = []

        let mutable protocolLogSubscriptions: IDisposable list = []

        let logSource = LogSource.Actor(name, actorUUId)

        let linkedActors = defaultArg linkedActorsOpt Seq.empty

        let logEvent = new Event<Log>()

        let principalActorProtocol = protocols.[0] :?> IPrincipalActorProtocol<'T>

        let actorRef = new ActorRef<'T>(actorUUId, name, protocols)

        let errorWrapBehavior (behavior: Actor<'T> -> Async<unit>) = fun () -> async {
                try
                    return! behavior self
                with e ->
                    logEvent.Trigger(Error, LogSource.Actor(name, actorUUId), new ActorFailedException("Actor behavior unhandled exception.", e) :> obj)

                    self.Stop()
            }

        new(name: string, behavior: Actor<'T> -> Async<unit>, ?linkedActorsOpt: seq<ActorBase>) = 
            let id = Guid.NewGuid()
            new Actor<'T>(id, name, [| new MailboxActorProtocol<_>(id, name) |], behavior, ?linkedActorsOpt = linkedActorsOpt)
            
        new(behavior: Actor<'T> -> Async<unit>, ?linkedActorsOpt: seq<ActorBase>) = new Actor<'T>("", behavior, ?linkedActorsOpt = linkedActorsOpt)

        new(otherActor: Actor<'T>) = 
            new Actor<'T>(otherActor.UUId, otherActor.Name, otherActor.Protocols, otherActor.Behavior, otherActor.LinkedActors)
        
        member private actor.Protocols = protocols
        member private actor.Behavior = behavior
        member private actor.LinkedActors = linkedActors

        member actor.UUId = actorUUId

        member actor.Name = name

        override actor.Log = logEvent.Publish

        member actor.CurrentQueueLength with get() = principalActorProtocol.CurrentQueueLength

        member private actor.Publish(newProtocolsF: ActorRef<'T> -> IActorProtocol<'T>[]) =
            let newId = ActorUUID.NewGuid()
            let mailboxProtocol = new MailboxActorProtocol<_>(newId, name)
            let actorRef = new ActorRef<'T>(newId, name, [| mailboxProtocol |])
            let newProtocols = newProtocolsF actorRef
                               |> Array.append (protocols |> Seq.map (fun protocol -> protocol.Configuration) 
                                                          |> Seq.choose id |> Seq.collect (fun conf -> conf.CreateProtocolInstances(actorRef)) 
                                                          |> Seq.toArray)
                               |> Array.append [| mailboxProtocol |]

            new Actor<'T>(newId, name, newProtocols, currentBehavior, ?linkedActorsOpt = linkedActorsOpt)

        abstract Publish: IActorProtocol<'T>[] -> Actor<'T>
        default actor.Publish(protocols': IActorProtocol<'T>[]) = 
            actor.Publish(fun _ -> protocols')

        abstract Publish: #seq<'U>  -> Actor<'T> when 'U :> IProtocolConfiguration
        default actor.Publish(configurations: #seq<'U> when 'U :> IProtocolConfiguration) =
            actor.Publish(fun actorRef -> configurations |> Seq.collect (fun conf -> conf.CreateProtocolInstances(actorRef)) |> Seq.toArray)
                           

        abstract Rename: string -> Actor<'T>
        default actor.Rename(newName: string): Actor<'T> =
            //first check new name
            if newName.Contains("/") then invalidArg "newName" "Actor names must not contain '/'."

            let newId = ActorUUID.NewGuid()
            let mailboxProtocol = new MailboxActorProtocol<_>(newId, newName)
            let actorRef = new ActorRef<'T>(newId, newName, [| mailboxProtocol |])

            let newProtocols = protocols |> Array.map (fun protocol -> protocol.Configuration)
                                         |> Array.choose id
                                         |> Array.collect (fun conf -> conf.CreateProtocolInstances(actorRef))
                                         |> Array.append [| mailboxProtocol |]
            
            new Actor<'T>(newId, newName, newProtocols, currentBehavior, ?linkedActorsOpt = linkedActorsOpt)
    
        abstract Ref: ActorRef<'T>
        default actor.Ref = actorRef

        abstract Receive: int -> Async<'T>
        default actor.Receive(timeout: int) = principalActorProtocol.Receive(timeout)

        abstract Receive: unit -> Async<'T>
        default actor.Receive() = actor.Receive(-1)
        
        abstract TryReceive: int -> Async<'T option>
        default actor.TryReceive(timeout: int) = principalActorProtocol.TryReceive(timeout)

        abstract TryReceive: unit -> Async<'T option>
        default actor.TryReceive() = actor.TryReceive(-1)

        abstract Scan: ('T -> Async<'U> option) * int -> Async<'U>
        default actor.Scan(scanner, timeout: int) = principalActorProtocol.Scan(scanner, timeout)

        abstract Scan: ('T -> Async<'U> option) -> Async<'U>
        default actor.Scan(scanner) = actor.Scan(scanner, -1)

        abstract TryScan: ('T -> Async<'U> option) * int -> Async<'U option>
        default actor.TryScan(scanner, timeout: int) = principalActorProtocol.TryScan(scanner, timeout)

        abstract TryScan: ('T -> Async<'U> option) -> Async<'U option>
        default actor.TryScan(scanner) = actor.TryScan(scanner, -1)
        
        override actor.LogEvent<'L>(logLevel: LogLevel, datum: 'L) = logEvent.Trigger(logLevel, logSource, datum :> obj)
        
        override actor.LogInfo<'L>(info: 'L) = actor.LogEvent(Info, info)

        override actor.LogWarning<'L>(warning: 'L) = actor.LogEvent(Warning, warning)

        override actor.LogError(e: exn) = actor.LogEvent(Error, e)

        member private actor.Start(behavior: Actor<'T> -> Async<unit>) =
            if not isStarted then
                linkedErrorRemoves <- [ for linkedActor in linkedActors -> linkedActor.Log |> Observable.subscribe logEvent.Trigger ]

                for linkedActor in linkedActors do
                    linkedActor.Start()

                protocolLogSubscriptions <- [ for protocol in protocols -> protocol.Log |> Observable.subscribe logEvent.Trigger ]

                principalActorProtocol.Start(errorWrapBehavior behavior)
                protocols |> Seq.skip 1 |> Seq.iter (fun protocol -> protocol.Start())

                isStarted <- true

        override actor.Start() = actor.Start(currentBehavior)
        
        override actor.Stop() = 
            if isStarted then
                for protocol in protocols |> Array.toList |> List.rev do protocol.Stop()

                for linkedErrorRemove in linkedErrorRemoves do linkedErrorRemove.Dispose()
                linkedErrorRemoves <- []
                
                for d in protocolLogSubscriptions do d.Dispose()
                protocolLogSubscriptions <- []

                for linkedActor in linkedActors do 
                    linkedActor.Stop()

                isStarted <- false

        abstract ReBind: (Actor<'T> -> Async<unit>) -> unit
        default actor.ReBind(behavior: Actor<'T> -> Async<unit>) =
            actor.Stop()
            currentBehavior <- behavior
            actor.Start(behavior)

        override actor.Equals(other: obj) =
            match other with
            | :? Actor<'T> as otherActor -> actor.Ref.Equals(otherActor.Ref)
            | _ -> false

        override actor.GetHashCode() = hash (actorUUId, name)

        interface IComparable with
            member actor.CompareTo(other: obj) = compareOn (fun (actor: Actor<'T>) -> actor.UUId) actor other

        interface IDisposable with
            member actor.Dispose() =
                actor.Stop()


    
    [<AutoOpen>]
    module Operators =

        let (<--) (actor: ActorRef<'T>) (msg: 'T) =
            actor.Post(msg)

        let (<-!-) (actor: ActorRef<'T>) (msg: 'T) =
            actor.PostAsync msg

        let (-->) (msg: 'T) (actorRef: ActorRef<'T>) =
            actorRef <-- msg

        let (<!-) (toRef: ActorRef<'T>) (msgBuilder: IReplyChannel<'R> -> 'T) =
            toRef.PostWithReply(msgBuilder)

        let (<!=) (toRef: ActorRef<'T>) (msgBuilder: IReplyChannel<'R> -> 'T) =
            toRef <!- msgBuilder |> Async.RunSynchronously

        let (!) (actor: Actor<'T>): ActorRef<'T> = actor.Ref

        //convenience for replying
        let nothing = Value ()
                

    module Actor =
        open Thespian.Utils

        let bind (body: Actor<'T> -> Async<unit>) =
            new Actor<'T>("", body)

        let empty(): Actor<'T> = bind (fun _ -> async.Zero())

        let sink(): Actor<'T> = bind (fun actor -> actor.Receive() |> Async.Ignore)

        let spawn (processFunc: 'T -> Async<unit>) =
            let rec body (actor: Actor<'T>) = async { 
                    let! msg = actor.Receive()
                    do! processFunc msg
                    return! body actor
                }
            bind body

        let bindLinked (body: Actor<'T> -> Async<unit>) (linkedActors: #seq<ActorBase>) =
            new Actor<'T>("", body, linkedActors)

        let spawnLinked (processFunc: 'T -> Async<unit>) (linkedActors: #seq<ActorBase>) =
            let rec body (actor: Actor<'T>) = async { 
                    let! msg = actor.Receive()
                    do! processFunc msg
                    return! body actor
                }
            bindLinked body linkedActors

        let publish (protocolConfs: #seq<'U> when 'U :> IProtocolConfiguration) (actor: Actor<'T>): Actor<'T> =
            actor.Publish(protocolConfs)

        let rename (newName: string) (actor: Actor<'T>): Actor<'T> =
            actor.Rename(newName)

        let start (actor: Actor<'T>): Actor<'T> =
            actor.Start()
            actor

        let intercept (interceptF: 'T -> Async<unit>) (actor: Actor<'T>): Actor<'T> =
            spawnLinked (fun msg -> async {                
                do! interceptF msg

                !actor <-- msg
            }) [actor]

        let filter (predicate: 'T -> bool) (actor: Actor<'T>): Actor<'T> =
            spawnLinked (fun msg -> async { if predicate msg then !actor <-- msg }) [actor]

        let broadcast (actors: #seq<Actor<'T>>): Actor<'T> =
            let rec broadcastBehavior (self: Actor<'T>) =
                async {
                    let! msg = self.Receive()

                    for actor in actors do
                        try
                            !actor <-- msg
                        with e -> self.LogWarning(e)

                    return! broadcastBehavior self
                }
            actors |> Seq.map (fun a -> a :> ActorBase) |> bindLinked broadcastBehavior

        
        let split (choiceF: 'T -> bool) (actorOnSuccess: Actor<'T>, actorOnFail: Actor<'T>): Actor<'T> =
            spawnLinked (fun msg -> async {
                if choiceF msg then !actorOnSuccess <-- msg
                else !actorOnFail <-- msg
            }) [actorOnSuccess; actorOnFail]

        let balance (select: seq<ActorRef<'T>> -> ActorRef<'T>) (actors: #seq<Actor<'T>>): Actor<'T> =
            let balancer msg = async {
                let selectedRef = actors |> Seq.map (!) |> select
                selectedRef <-- msg
            }
            spawnLinked balancer (actors |> Seq.map (fun a -> a :> ActorBase))

        let map (mapF: 'U -> 'T) (actor: Actor<'T>) : Actor<'U> =
            spawnLinked (fun msg -> async {
                !actor <-- (mapF msg) 
            }) [actor]
        
        let partition (partitionF: 'T -> int) (actors: #seq<Actor<'T>>): Actor<'T> =
            spawnLinked (fun msg -> async {
                let selected = partitionF msg
                !(actors |> Seq.nth selected) <-- msg
            }) (actors |> Seq.map (fun a -> a :> ActorBase))

        let scatter (choiceF: 'T -> (IReplyChannel<'R> * (IReplyChannel<'R> -> 'T)) option) 
            (gatherF: seq<Reply<'R>> -> Reply<'R>) 
            (actors: #seq<Actor<'T>>): Actor<'T> =
    
            spawnLinked (fun msg -> async {
                match choiceF msg with
                | Some(replyChannel, msgBuilder) -> 
                    let! results =
                        [ for actor in actors -> async {
                                try
                                    let! result = !actor <!- msgBuilder
                                    return Value result
                                with e ->
                                    return Reply.Exception e
                            }
                        ] |> Async.Parallel
                    results |> Array.toSeq |> gatherF |> replyChannel.Reply
                | None -> ()
            }) (actors |> Seq.map (fun a -> a :> ActorBase))
                
        let ref (actor: Actor<'T>): ActorRef<'T> =
            !actor
            
        let wrapRef (actorRef: ActorRef<'T>): Actor<'T> =
            let rec body (self: Actor<'T>) = async {
                let! msg = self.Receive()
                actorRef <-- msg

                return! body self
            }

            {
                new Actor<'T>(body) with
                    override actor.Ref = actorRef
            }

        let spawnMany (processFunc: 'T -> Async<unit>) (howMany: int) (balancer: seq<ActorRef<'T>> -> ActorRef<'T>): Actor<'T> =
            [ for i in 1..howMany -> spawn processFunc ] |> balance balancer

        let box (actor: Actor<'T>): Actor<obj> =
            actor |> map (fun msg -> msg :?> 'T)

        let unbox (actor: Actor<obj>): Actor<'T> =
            actor |> map (fun msg -> msg :> obj)

        let union (actor: Actor<'T>) (actor': Actor<'U>): Actor<Choice<'T, 'U>> =
            spawnLinked (fun msg -> async {
                match msg with
                | Choice1Of2 m -> !actor <-- m
                | Choice2Of2 m -> !actor' <-- m
            }) [actor |> box; actor' |> box]

    [<AutoOpen>]
    module ActorRefExtensions =
        module ActorRef =
            let configurationFilter (filterF: IProtocolConfiguration -> bool) (actorRef: ActorRef<'T>): ActorRef<'T> =
                actorRef.ConfigurationFilter filterF

            let empty() = Actor.sink() |> Actor.ref

    module Behavior =
        module WithSelf =
            let rec stateful (state: 'U) (behavior: 'U -> ActorRef<'T> -> 'T -> Async<'U>) (self: Actor<'T>) =
                async {
                    let! msg = self.Receive()

                    let! state' = behavior state !self msg

                    return! stateful state' behavior self
                }

            let stateless (behavior: ActorRef<'T> -> 'T -> Async<unit>) =
                stateful () (fun _ self msg -> behavior self msg)

        let rec stateful (state: 'U) (behavior: 'U -> 'T -> Async<'U>) =
            WithSelf.stateful state (fun state _ msg -> behavior state msg)

        let rec stateful2 (state: 'U) (behavior: 'U -> Actor<'T> -> Async<'U>) (self : Actor<'T>) =
            async {
                let! state' = behavior state self

                return! stateful2 state' behavior self
            }

        let stateless (behavior: 'T -> Async<unit>) (self: Actor<'T>) =
            stateful () (fun _ msg -> behavior msg) self
