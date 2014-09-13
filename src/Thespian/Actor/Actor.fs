namespace Nessos.Thespian

open System
open System.Threading

open Nessos.Thespian.Utilities


type IPrimaryProtocolFactory =
    abstract Create: string -> IPrimaryProtocolServer<'T>

type MailboxPrimaryProtocolFactory() =
    interface IPrimaryProtocolFactory with override __.Create(actorName: string) = new Mailbox.MailboxProtocolServer<'T>(actorName) :> IPrimaryProtocolServer<'T>

[<AbstractClass>]
type Actor() =
    [<VolatileField>]
    static let mutable primaryProtocolFactory = new MailboxPrimaryProtocolFactory() :> IPrimaryProtocolFactory

    static member DefaultPrimaryProtocolFactory with get() = primaryProtocolFactory
                                                 and set f = primaryProtocolFactory <- f
    
    abstract Log: IEvent<Log>
    abstract LogEvent: LogLevel * 'E -> unit
    abstract LogInfo: 'I -> unit
    abstract LogWarning: 'W -> unit
    abstract LogError: exn -> unit
    abstract Start: unit -> unit
    abstract Stop: unit -> unit

type Actor<'T>(name: string, protocols: IProtocolServer<'T>[], behavior: Actor<'T> -> Async<unit>, ?linkedActors: seq<Actor>) as self =
    inherit Actor()
    
    do
        if name.Contains("/") then invalidArg "name" "Actor names must not contain '/'."
        if protocols = Array.empty then invalidArg "protocols" "No protocols specified."
    
        match protocols.[0] with
        | :? IPrimaryProtocolServer<'T> -> ()
        | _ -> invalidArg "protocols" "First protocol is not primary."
    
        if protocols |> Array.exists (fun protocol -> protocol.ActorId.Name <> name) then invalidArg "protocols" "Name mismatch in protocols."
    
    let mutable currentBehavior = behavior
    let mutable isStarted = false        
    let mutable linkedErrorRemoves: IDisposable list = []
    let mutable protocolLogSubscriptions: IDisposable list = []
    let logSource = LogSource.Actor name
    let linkedActors = defaultArg linkedActors Seq.empty
    let logEvent = new Event<Log>()
    let primaryProtocol = protocols.[0] :?> IPrimaryProtocolServer<'T>
    let clientProtocols = protocols |> Array.map (fun protocol -> protocol.Client)
    let actorRef = new ActorRef<'T>(name, clientProtocols)
    
    let errorWrapBehavior (behavior: Actor<'T> -> Async<unit>) =
        fun () -> async {
            try return! behavior self
            with e ->
                logEvent.Trigger(Error, LogSource.Actor name, new ActorFailedException("Actor behavior unhandled exception.", e) :> obj)
                self.Stop()
        }
    
    new (name: string, behavior: Actor<'T> -> Async<unit>, ?linkedActors: seq<Actor>) = new Actor<'T>(name, [| Actor.DefaultPrimaryProtocolFactory.Create name |], behavior, ?linkedActors = linkedActors)
    new (behavior: Actor<'T> -> Async<unit>, ?linkedActors: seq<Actor>) = new Actor<'T>(String.Empty, behavior, ?linkedActors = linkedActors)
    new (otherActor: Actor<'T>) = new Actor<'T>(otherActor.Name, otherActor.Protocols, otherActor.Behavior, otherActor.LinkedActors)
    
    member private __.Protocols = protocols
    member private __.Behavior = behavior
    member private __.LinkedActors = linkedActors
    
    member __.Name = name
    
    override __.Log = logEvent.Publish
    
    member __.PendingMessages with get() = primaryProtocol.PendingMessages
    
    member private __.Publish(newProtocolsF: ActorRef<'T> -> IProtocolServer<'T>[]) =
        let mailboxProtocol = new Mailbox.MailboxProtocolServer<_>(name) :> IPrimaryProtocolServer<_>
        let actorRef = new ActorRef<'T>(name, [| mailboxProtocol.Client |])
        let newProtocols = newProtocolsF actorRef
                           |> Array.append (protocols |> Seq.map (fun protocol -> protocol.Client.Factory)
                                                      |> Seq.choose id
                                                      |> Seq.map (fun factory -> factory.CreateServerInstance<_>(name, actorRef))
                                                      |> Seq.toArray)
                           |> Array.append [| mailboxProtocol |]
    
        new Actor<'T>(name, newProtocols, currentBehavior, linkedActors)
    
    abstract Publish: IProtocolServer<'T>[] -> Actor<'T>
    default self.Publish(protocols': IProtocolServer<'T>[]) = self.Publish(fun _ -> protocols')
    
    abstract Publish: #seq<'U>  -> Actor<'T> when 'U :> IProtocolFactory
    default self.Publish(protocolFactories: #seq<'U> when 'U :> IProtocolFactory) =
        self.Publish(fun actorRef -> protocolFactories |> Seq.map (fun factory -> factory.CreateServerInstance<'T>(name, actorRef)) |> Seq.toArray)
    
    abstract Rename: string -> Actor<'T>
    default actor.Rename(newName: string): Actor<'T> =
        //first check new name
        if newName.Contains("/") then invalidArg "newName" "Actor names must not contain '/'."
    
        let mailboxProtocol = new Mailbox.MailboxProtocolServer<_>(newName) :> IPrimaryProtocolServer<_>
        let actorRef = new ActorRef<'T>(newName, [| mailboxProtocol.Client |])
    
        let newProtocols = protocols |> Array.map (fun protocol -> protocol.Client.Factory)
                                     |> Array.choose id
                                     |> Array.map (fun factory -> factory.CreateServerInstance<_>(name, actorRef))
                                     |> Array.append [| mailboxProtocol |]
    
        new Actor<'T>(newName, newProtocols, currentBehavior, linkedActors)
    
    abstract Ref: ActorRef<'T>
    default actor.Ref = actorRef
    
    abstract Receive: ?timeout: int -> Async<'T>
    default __.Receive(?timeout: int) = let timeout = defaultArg timeout Timeout.Infinite in primaryProtocol.Receive(timeout)
    
    abstract TryReceive: ?timeout: int -> Async<'T option>
    default __.TryReceive(?timeout: int) = let timeout = defaultArg timeout Timeout.Infinite in primaryProtocol.TryReceive(timeout)
    
    override __.LogEvent<'L>(logLevel: LogLevel, datum: 'L) = logEvent.Trigger(logLevel, logSource, datum :> obj)
    
    override self.LogInfo<'L>(info: 'L) = self.LogEvent(Info, info)
    
    override self.LogWarning<'L>(warning: 'L) = self.LogEvent(Warning, warning)
    
    override self.LogError(e: exn) = self.LogEvent(Error, e)
    
    member private __.Start(behavior: Actor<'T> -> Async<unit>) =
        if not isStarted then
            linkedErrorRemoves <- [ for linkedActor in linkedActors -> linkedActor.Log |> Observable.subscribe logEvent.Trigger ]
    
            for linkedActor in linkedActors do linkedActor.Start()
    
            protocolLogSubscriptions <- [ for protocol in protocols -> protocol.Log |> Observable.subscribe logEvent.Trigger ]
    
            primaryProtocol.Start(errorWrapBehavior behavior)
            protocols |> Seq.skip 1 |> Seq.iter (fun protocol -> protocol.Start())
            isStarted <- true
    
    override self.Start() = self.Start(currentBehavior)
    
    override __.Stop() = 
        if isStarted then
            for protocol in protocols |> Array.toList |> List.rev do protocol.Stop()
    
            for linkedErrorRemove in linkedErrorRemoves do linkedErrorRemove.Dispose()
            linkedErrorRemoves <- []
    
            for d in protocolLogSubscriptions do d.Dispose()
            protocolLogSubscriptions <- []
    
            for linkedActor in linkedActors do linkedActor.Stop()
            isStarted <- false
    
    abstract ReBind: (Actor<'T> -> Async<unit>) -> unit
    default self.ReBind(behavior: Actor<'T> -> Async<unit>) =
        if isStarted then
            self.Stop()
            currentBehavior <- behavior
            self.Start(behavior)
        else
            currentBehavior <- behavior
    
    interface IDisposable with override self.Dispose() = self.Stop()


    
[<AutoOpen>]
module Operators =

    let inline (<--) (actorRef: ActorRef<'T>) (msg: 'T) = actorRef.Post(msg)

    let inline (<-!-) (actorRef: ActorRef<'T>) (msg: 'T) = actorRef.AsyncPost msg

    let inline (-->) (msg: 'T) (actorRef: ActorRef<'T>) = actorRef <-- msg

    let inline (<!-) (actorRef: ActorRef<'T>) (msgF: IReplyChannel<'R> -> 'T) = actorRef.PostWithReply(msgF)

    let inline (<!=) (actorRef: ActorRef<'T>) (msgF: IReplyChannel<'R> -> 'T) = actorRef <!- msgF |> Async.RunSynchronously

    let inline (!) (actor: Actor<'T>): ActorRef<'T> = actor.Ref

    //convenience for replying
    let nothing = Value ()
                
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Actor =
    open Nessos.Thespian.Utilities

    let inline bind (body: Actor<'T> -> Async<unit>) = let name = Guid.NewGuid().ToString() in new Actor<'T>(name, body)

    let inline empty(): Actor<'T> = bind (fun _ -> async.Zero())
    let inline sink(): Actor<'T> = bind (fun actor -> actor.Receive() |> Async.Ignore)

    let inline spawn (processFunc: 'T -> Async<unit>) =
        let rec body (actor: Actor<'T>) = async { 
                let! msg = actor.Receive()
                do! processFunc msg
                return! body actor
            }
        bind body

    let inline bindLinked (body: Actor<'T> -> Async<unit>) (linkedActors: #seq<Actor>) = new Actor<'T>(String.Empty, body, linkedActors)

    let inline spawnLinked (processFunc: 'T -> Async<unit>) (linkedActors: #seq<Actor>) =
        let rec body (actor: Actor<'T>) = async { 
                let! msg = actor.Receive()
                do! processFunc msg
                return! body actor
            }
        bindLinked body linkedActors

    let inline publish (protocolFactories: #seq<'U> when 'U :> IProtocolFactory) (actor: Actor<'T>): Actor<'T> = actor.Publish(protocolFactories)

    let inline rename (newName: string) (actor: Actor<'T>): Actor<'T> = actor.Rename(newName)

    let inline start (actor: Actor<'T>): Actor<'T> = actor.Start(); actor

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
        actors |> Seq.map (fun a -> a :> Actor) |> bindLinked broadcastBehavior


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
        spawnLinked balancer (actors |> Seq.map (fun a -> a :> Actor))

    let map (mapF: 'U -> 'T) (actor: Actor<'T>) : Actor<'U> =
        spawnLinked (fun msg -> async {
            !actor <-- (mapF msg) 
        }) [actor]

    let partition (partitionF: 'T -> int) (actors: #seq<Actor<'T>>): Actor<'T> =
        spawnLinked (fun msg -> async {
            let selected = partitionF msg
            !(actors |> Seq.nth selected) <-- msg
        }) (actors |> Seq.map (fun a -> a :> Actor))

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
        }) (actors |> Seq.map (fun a -> a :> Actor))

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
        let inline configurationFilter (filterF: IProtocolFactory -> bool) (actorRef: ActorRef<'T>): ActorRef<'T> = actorRef.ProtocolFilter filterF
        let inline empty() = Actor.sink() |> Actor.ref

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
      
    let rec stateful (state: 'U) (behavior: 'U -> 'T -> Async<'U>) = WithSelf.stateful state (fun state _ msg -> behavior state msg)

    let rec stateful2 (state: 'U) (behavior: 'U -> Actor<'T> -> Async<'U>) (self : Actor<'T>) =
        async {
            let! state' = behavior state self

            return! stateful2 state' behavior self
        }

    let stateless (behavior: 'T -> Async<unit>) (self: Actor<'T>) =
        stateful () (fun _ msg -> behavior msg) self
