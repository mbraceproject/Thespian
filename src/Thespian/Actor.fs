namespace Nessos.Thespian

open System
open System.Threading

open Nessos.Thespian.Logging

/// Represents an untyped locally executing actor instance.
[<AbstractClass>]
type Actor() =
    [<VolatileField>]
    static let mutable primaryProtocolFactory = new MailboxProtocol.MailboxPrimaryProtocolFactory() :> IPrimaryProtocolFactory

    /// Gets or sets the default primary protocol used by actors.
    static member DefaultPrimaryProtocolFactory 
        with get() = primaryProtocolFactory
        and set f = primaryProtocolFactory <- f
    
    /// Actor event observable
    abstract Log: IEvent<Log>

    /// <summary>
    ///     Trigger a new event for given actor.
    /// </summary>
    /// <param name="logLevel">Log level.</param>
    /// <param name="event">event value.</param>
    abstract LogEvent: logLevel:LogLevel * event:'E -> unit

    /// Starts given actor instance.
    abstract Start: unit -> unit
    /// Stops given actor instance.
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
    let linkedActors = match linkedActors with None -> [||] | Some la -> Seq.toArray la
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

    new (name: string, behavior: Actor<'T> -> Async<unit>, ?primaryProtocolFactory: IPrimaryProtocolFactory, ?linkedActors: seq<Actor>) =
      let primaryProtocolFactory = defaultArg primaryProtocolFactory Actor.DefaultPrimaryProtocolFactory
      new Actor<'T>(name, [| primaryProtocolFactory.Create name :> IProtocolServer<'T> |], behavior, ?linkedActors = linkedActors)
    new (behavior: Actor<'T> -> Async<unit>, ?primaryProtocolFactory: IPrimaryProtocolFactory, ?linkedActors: seq<Actor>) = new Actor<'T>(String.Empty, behavior, ?primaryProtocolFactory = primaryProtocolFactory, ?linkedActors = linkedActors)
    new (otherActor: Actor<'T>) = new Actor<'T>(otherActor.Name, protocols = otherActor.Protocols, behavior = otherActor.Behavior, linkedActors = otherActor.LinkedActors)
    
    /// Protocol server instances used by the actor
    member private __.Protocols = protocols
    /// Asynchronous, tail-recursive loop behaviour used by actor.
    member private __.Behavior = behavior
    /// Actors linked to the given actor instance.
    member private __.LinkedActors = linkedActors
    
    /// Actor name
    member __.Name = name
    
    override __.Log = logEvent.Publish
    
    /// Return the number of messages pending processing by actor instance.
    member __.PendingMessages = primaryProtocol.PendingMessages
    
    member private __.Publish(newProtocolsF: ActorRef<'T> -> IProtocolServer<'T>[]) =
        let primaryProtocol' = primaryProtocol.CreateInstance(name)
        let actorRef = new ActorRef<'T>(name, [| primaryProtocol'.Client |])
        let newProtocols = newProtocolsF actorRef
                           |> Array.append (protocols |> Seq.map (fun protocol -> protocol.Client.Factory)
                                                      |> Seq.choose id
                                                      |> Seq.map (fun factory -> factory.CreateServerInstance<_>(name, actorRef))
                                                      |> Seq.toArray)
                           |> Array.append [| primaryProtocol' |]
    
        new Actor<'T>(name, newProtocols, currentBehavior, linkedActors)
    
    /// <summary>
    ///     Publishes given actor to collection of protocol server instances.
    /// </summary>
    /// <param name="protocolServers">Protocol servers to be published to.</param>
    abstract Publish: protocolServers:IProtocolServer<'T>[] -> Actor<'T>
    default self.Publish(protocols': IProtocolServer<'T>[]) = self.Publish(fun _ -> protocols')
    
    /// <summary>
    ///     Publishes given actor to collection of protocol factories.
    /// </summary>
    /// <param name="protocolFactories">Protocol factories to be published to.</param>
    abstract Publish: protocolFactories:#seq<'U>  -> Actor<'T> when 'U :> IProtocolFactory
    default self.Publish(protocolFactories: #seq<'U> when 'U :> IProtocolFactory) =
        self.Publish(fun actorRef -> protocolFactories |> Seq.map (fun factory -> factory.CreateServerInstance<'T>(name, actorRef)) |> Seq.toArray)
    
    /// <summary>
    ///     Creates a renamed copy of given actor.
    /// </summary>
    /// <param name="newName">new name for given actor.</param>
    abstract Rename: newName:string -> Actor<'T>
    default actor.Rename(newName: string): Actor<'T> =
        //first check new name
        if newName.Contains("/") then invalidArg "newName" "Actor names must not contain '/'."
    
        let primaryProtocol' = primaryProtocol.CreateInstance(newName)
        let actorRef = new ActorRef<'T>(newName, [| primaryProtocol'.Client |])
    
        let newProtocols = protocols |> Array.map (fun protocol -> protocol.Client.Factory)
                                     |> Array.choose id
                                     |> Array.map (fun factory -> factory.CreateServerInstance<_>(newName, actorRef))
                                     |> Array.append [| primaryProtocol' |]
    
        new Actor<'T>(newName, newProtocols, currentBehavior, linkedActors)
    
    /// Gets a reference to given actor instance.
    abstract Ref: ActorRef<'T>
    default actor.Ref = actorRef
    
    /// <summary>
    ///     Asynchronously dequeue a message from actor inbox.
    /// </summary>
    /// <param name="timeout">Timeout in milliseconds. Defaults to infinite.</param>
    abstract Receive: ?timeout: int -> Async<'T>
    default __.Receive(?timeout: int) = let timeout = defaultArg timeout Timeout.Infinite in primaryProtocol.Receive(timeout)
    
    /// <summary>
    ///     Asynchronously dequeue a message from actor inbox. Return 'None' on timeout.
    /// </summary>
    /// <param name="timeout">Timeout in milliseconds. Defaults to infinite.</param>
    abstract TryReceive: ?timeout: int -> Async<'T option>
    default __.TryReceive(?timeout: int) = let timeout = defaultArg timeout Timeout.Infinite in primaryProtocol.TryReceive(timeout)
    
    override __.LogEvent<'L>(logLevel: LogLevel, datum: 'L) = logEvent.Trigger(logLevel, logSource, datum :> obj)
    
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


/// Extension methods for Actor types.
[<AutoOpen>]
module ActorExtensions =
    
    type Actor with
        /// <summary>
        ///     Log information event.
        /// </summary>
        /// <param name="info">info value.</param>
        member self.LogInfo<'L>(info: 'L) = self.LogEvent(Info, info)

        /// <summary>
        ///     Log warning event.
        /// </summary>
        /// <param name="warning">warning value.</param>
        member self.LogWarning<'L>(warning: 'L) = self.LogEvent(Warning, warning)

        /// <summary>
        ///     Log exception event.
        /// </summary>
        /// <param name="exn">exception value.</param>
        member self.LogError(exn : exn) = self.LogEvent(Error, exn)
 
 /// Collection of operators acting on actor types.
[<AutoOpen>]
module Operators =

    /// <summary>
    ///     Synchronously post message to actor reference.
    /// </summary>
    /// <param name="actorRef">Recipient actor.</param>
    /// <param name="msg">Message.</param>
    let inline (<--) (actorRef: ActorRef<'T>) (msg: 'T) = actorRef.Post(msg)

    /// <summary>
    ///     Asynchronously post message to actor reference.
    /// </summary>
    /// <param name="actorRef">Recipient actor.</param>
    /// <param name="msg">Message.</param>
    let inline (<-!-) (actorRef: ActorRef<'T>) (msg: 'T) = actorRef.AsyncPost msg

    /// <summary>
    ///     Synchronously post message to actor reference.
    /// </summary>
    /// <param name="actorRef">Recipient actor.</param>
    /// <param name="msg">Message.</param>
    let inline (-->) (msg: 'T) (actorRef: ActorRef<'T>) = actorRef <-- msg

    /// <summary>
    ///     Post message to actor reference and asynchronously await for reply.
    /// </summary>
    /// <param name="actorRef">Recipient actor.</param>
    /// <param name="msg">Message builder.</param>
    let inline (<!-) (actorRef: ActorRef<'T>) (msgF: IReplyChannel<'R> -> 'T) = actorRef.PostWithReply(msgF)

    /// <summary>
    ///     Post message to actor reference and asynchronously await for reply.
    /// </summary>
    /// <param name="actorRef">Recipient actor.</param>
    /// <param name="msg">Message builder.</param>
    let inline (<!=) (actorRef: ActorRef<'T>) (msgF: IReplyChannel<'R> -> 'T) = actorRef <!- msgF |> Async.RunSynchronously

    /// <summary>
    ///     Gets actor reference for given actor.
    /// </summary>
    /// <param name="actor">Actor to get reference from.</param>
    let inline (!) (actor: Actor<'T>): ActorRef<'T> = actor.Ref
              
/// Collection of actor combinators
[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Actor =

    /// <summary>
    ///     Instantiates an asynchronous, tail recursive actor behaviour workflow into an inactive actor instance.
    /// </summary>
    /// <param name="body">Actor behaviour workflow.</param>
    let inline bind (body: Actor<'T> -> Async<unit>) = let name = Guid.NewGuid().ToString() in new Actor<'T>(name, body)

    /// Creates a trivial actor that exits.
    let inline empty() : Actor<'T> = bind (fun _ -> async.Zero())

    // this appears to be wrong: will receive one message and then exit, should probably discard everything it receives.

    /// Creates a trivial actor that receives a single message and exits.
    let inline sink() : Actor<'T> = bind (fun actor -> actor.Receive() |> Async.Ignore)

    /// <summary>
    ///     Wraps an asynchronous reactive workflow into a tail-recursive actor definition.
    /// </summary>
    /// <param name="reactF">Worfklow that reacts on input messages.</param>
    let inline spawn (reactF: 'T -> Async<unit>) =
        let rec body (actor: Actor<'T>) = async { 
                let! msg = actor.Receive()
                do! reactF msg
                return! body actor
            }
        bind body

    /// <summary>
    ///     Instantiates an asynchronous, tail recursive actor behaviour workflow into an inactive actor instance.
    /// </summary>
    /// <param name="body">Actor behaviour workflow.</param>
    /// <param name="linkedActors">Actors linked to current instance.</param>
    let inline bindLinked (body: Actor<'T> -> Async<unit>) (linkedActors: #seq<Actor>) = 
        new Actor<'T>(String.Empty, body, linkedActors = linkedActors)

    /// <summary>
    ///     Wraps an asynchronous reactive workflow into a tail-recursive actor definition.
    /// </summary>
    /// <param name="reactF">Worfklow that reacts on input messages.</param>
    /// <param name="linkedActors">Actors linked to current instance.</param>
    let inline spawnLinked (reactF : 'T -> Async<unit>) (linkedActors: #seq<Actor>) =
        let rec body (actor: Actor<'T>) = async { 
                let! msg = actor.Receive()
                do! reactF msg
                return! body actor
            }
        bindLinked body linkedActors

    /// <summary>
    ///     Publish given actor implementation to provided protocol implementation factories.
    /// </summary>
    /// <param name="protocolFactories">Protocol factories.</param>
    /// <param name="actor">Target actor.</param>
    let inline publish (protocolFactories: #seq<'U> when 'U :> IProtocolFactory) (actor: Actor<'T>): Actor<'T> = 
        actor.Publish(protocolFactories)

    /// <summary>
    ///     Creates a copy of the actor instance with a new name.
    /// </summary>
    /// <param name="newName">New actor name.</param>
    /// <param name="actor">Target actor.</param>
    let inline rename (newName: string) (actor: Actor<'T>): Actor<'T> = actor.Rename(newName)

    /// <summary>
    ///     Starts the given actor instance.
    /// </summary>
    /// <param name="actor">Actor to be started.</param>
    let inline start (actor: Actor<'T>): Actor<'T> = actor.Start(); actor

    /// <summary>
    ///     Creates a new actor that intercepts messages
    ///     before forwarding them to the target actor.
    /// </summary>
    /// <param name="interceptF">Interception behavior.</param>
    /// <param name="actor">Target actor.</param>
    let intercept (interceptF: 'T -> Async<unit>) (actor: Actor<'T>): Actor<'T> =
        spawnLinked (fun msg -> async {                
            do! interceptF msg

            return! !actor <-!- msg
        }) [actor]

    /// <summary>
    ///     Creates a new actor that only forwards messages
    ///     which satisfy given predicate.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="actor">Target actor.</param>
    let filter (predicate: 'T -> bool) (actor: Actor<'T>): Actor<'T> =
        spawnLinked (fun msg -> async { if predicate msg then return! !actor <-!- msg }) [actor]

    /// <summary>
    ///     Creates a new actor that broadcasts message received to
    ///     all target actors.
    /// </summary>
    /// <param name="actors">Target actors.</param>
    let broadcast (actors: #seq<Actor<'T>>): Actor<'T> =
        let rec broadcastBehavior (self: Actor<'T>) =
            async {
                let! msg = self.Receive()

                let send actor = async {
                    try
                        return! !actor <-!- msg
                    with e -> self.LogWarning e
                }

                do! actors |> Seq.map send |> Async.Parallel |> Async.Ignore

                return! broadcastBehavior self
            }
        actors |> Seq.map (fun a -> a :> Actor) |> bindLinked broadcastBehavior

    /// <summary>
    ///     Creates a new actor that passes incoming messages through a choice predicate;
    ///     successful inputs are passed to the first actor, whereas failed will be passed to
    ///     the failed actor.
    /// </summary>
    /// <param name="choiceF">Choice predicate.</param>
    /// <param name="actorOnSuccess">Actor to forward message on true.</param>
    /// <param name="actorOnFail">Actor to forward message on false.</param>
    let split (choiceF: 'T -> bool) (actorOnSuccess: Actor<'T>, actorOnFail: Actor<'T>): Actor<'T> =
        spawnLinked (fun msg -> async {
            if choiceF msg then return! !actorOnSuccess <-!- msg
            else return! !actorOnFail <-!- msg
        }) [actorOnSuccess; actorOnFail]

    /// <summary>
    ///     Balance message passing across a collection of actors using a selection function.
    /// </summary>
    /// <param name="select">Selection function.</param>
    /// <param name="actors">Target actors.</param>
    let balance (select: seq<ActorRef<'T>> -> ActorRef<'T>) (actors: #seq<Actor<'T>>): Actor<'T> =
        let actors = Seq.toArray actors
        let balancer msg = async {
            let selectedRef = actors |> Seq.map (!) |> select
            return! selectedRef <-!- msg
        }
        spawnLinked balancer (actors |> Array.map (fun a -> a :> Actor))

    /// <summary>
    ///     Creates an actor that maps messages to target actor.
    /// </summary>
    /// <param name="mapF">Mapping function.</param>
    /// <param name="actor">Target actor.</param>
    let map (mapF: 'U -> 'T) (actor: Actor<'T>) : Actor<'U> =
        spawnLinked (fun msg -> async {
            return! !actor <-!- (mapF msg) 
        }) [actor]

    /// <summary>
    ///     Creates an actor that forwards to target actors according to an indexing function.
    /// </summary>
    /// <param name="partitionF">Indexing function.</param>
    /// <param name="actors">Target actors.</param>
    let partition (partitionF: 'T -> int) (actors: #seq<Actor<'T>>): Actor<'T> =
        let actors = Seq.toArray actors
        spawnLinked (fun msg -> async {
            let selected = partitionF msg
            return! !actors.[selected] <-!- msg
        }) (actors |> Array.map (fun a -> a :> Actor))


//    let scatter (choiceF: 'T -> (IReplyChannel<'R> * (IReplyChannel<'R> -> 'T)) option) 
//        (gatherF: seq<Reply<'R>> -> Reply<'R>) 
//        (actors: #seq<Actor<'T>>): Actor<'T> =
//
//        spawnLinked (fun msg -> async {
//            match choiceF msg with
//            | Some(replyChannel, msgBuilder) -> 
//                let! results =
//                    [ for actor in actors -> async {
//                            try
//                                let! result = !actor <!- msgBuilder
//                                return Value result
//                            with e ->
//                                return Exn e
//                        }
//                    ] |> Async.Parallel
//                return! results |> Array.toSeq |> gatherF |> replyChannel.AsyncReply
//            | None -> ()
//        }) (actors |> Seq.map (fun a -> a :> Actor))

    /// <summary>
    ///     Gets reference to given actor.
    /// </summary>
    /// <param name="actor">Input actor.</param>
    let inline ref (actor: Actor<'T>): ActorRef<'T> = actor.Ref

    /// <summary>
    ///     Creates a local actor instance that forwards messages to target ActorRef.
    /// </summary>
    /// <param name="actorRef">Target ActorRef.</param>
    let wrapRef (actorRef: ActorRef<'T>): Actor<'T> =
        let rec body (self: Actor<'T>) = async {
            let! msg = self.Receive()
            do! actorRef <-!- msg

            return! body self
        }

        {
            new Actor<'T>(body) with
                override actor.Ref = actorRef
        }


//    let spawnMany (processFunc: 'T -> Async<unit>) (howMany: int) (balancer: seq<ActorRef<'T>> -> ActorRef<'T>): Actor<'T> =
//        [ for i in 1..howMany -> spawn processFunc ] |> balance balancer

    /// <summary>
    ///     Creates a new actor that forwards boxed messages to target.
    /// </summary>
    /// <param name="actor">Target actor.</param>
    let box (actor: Actor<'T>): Actor<obj> =
        actor |> map (fun msg -> msg :?> 'T)

    /// <summary>
    ///     Creates a new actor that forwards unboxed messages to target.
    /// </summary>
    /// <param name="actor">Target actor.</param>
    let unbox (actor: Actor<obj>): Actor<'T> =
        actor |> map (fun msg -> msg :> obj)

    /// <summary>
    ///     Fuse constituent actors into one that accepts union types.
    /// </summary>
    /// <param name="actor"></param>
    /// <param name="actor'"></param>
    let union (actor: Actor<'T>) (actor': Actor<'U>): Actor<Choice<'T, 'U>> =
        spawnLinked (fun msg -> async {
            match msg with
            | Choice1Of2 m -> return! !actor <-!- m
            | Choice2Of2 m -> return! !actor' <-!- m
        }) [actor |> box; actor' |> box]

/// Collection of ActorRef combinators
[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module ActorRef =
        
    /// <summary>
    ///     Filters actor ref protocols by predicate.
    /// </summary>
    /// <param name="filterF">Protocol filtering function.</param>
    /// <param name="actorRef">Input ActorRef.</param>
    /// <returns>ActorRef with filtered protocol configurations.</returns>
    let inline configurationFilter (filterF: IProtocolFactory -> bool) (actorRef: ActorRef<'T>): ActorRef<'T> = actorRef.ProtocolFilter filterF

    /// <summary>
    ///     Return reference to a new actor that accepts one message and then exits.
    /// </summary>
    let inline empty() = Actor.sink() |> Actor.ref


/// Collection of behaviour-based combinators
[<RequireQualifiedAccess>]
module Behavior =

    /// <summary>
    ///     Creates a stateful behaviour workflow out of provided components.
    /// </summary>
    /// <param name="init">Initial state.</param>
    /// <param name="behavior">State-updating reactive asynchronous workflow.</param>
    /// <param name="self">'This' actor.</param>
    let rec stateful (init: 'State) (behavior: 'State -> 'T -> Async<'State>) (self: Actor<'T>) : Async<unit> =
        async {
            let! msg = self.Receive()

            let! state' = behavior init msg

            return! stateful state' behavior self
        }

    /// <summary>
    ///     Creates a stateless behaviour workflow out of a message-reactive function.
    /// </summary>
    /// <param name="reactF">workflow that reacts asynchronously to input message.</param>
    let stateless (behavior: 'T -> Async<unit>) (self: Actor<'T>) =
        stateful () (fun _ msg -> behavior msg) self