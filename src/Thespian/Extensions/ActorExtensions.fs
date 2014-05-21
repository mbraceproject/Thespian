namespace Nessos.Thespian.ActorExtensions

    open System
    open System.Net
    open System.Runtime.Serialization

    open Nessos.Thespian
    open Nessos.Thespian.ConcurrencyTools
    open Nessos.Thespian.Serialization

    type Rely<'T> = IReplyChannel<unit> * 'T

    type Replicated<'T> =
        | Replicated of Rely<'T>
        | Singular of 'T

    type AsyncReplicated<'T> = 
        | AsyncReplicated of 'T
        | SyncReplicated of Rely<'T>
        | SyncSingular of 'T

    type Guarantee<'T>(confirmChannel: IReplyChannel<unit>, msg: 'T) =
        let mutable isDisposed = false

        member g.Payload 
            with get() =
                if isDisposed then
                    raise (new ObjectDisposedException("Guarantee", "Cannot access the payload of a disposed guaranteed message."))

                msg

        interface IDisposable with
            member g.Dispose() = 
                if not isDisposed then
                    confirmChannel.Reply nothing
                    isDisposed <- true

    type Suicidal<'T> =
        | Msg of 'T 
        | Suicide

    type Controllable<'T> =
        | Msg of 'T
        | Override of 'T
        | Pause
        | Resume

    type Swapable<'T> =
        | Msg of 'T
        //USE ONLY WITH IN-MEMORY ACTORS
        | Swap of IReplyChannel<unit> * Actor<'T>

    type RawOrNormal<'T> = 
        | RawMessage of byte[]
        | NormalMessage of 'T

    [<Serializable>]
    type Raw<'T> =
        val private rawValue: RawOrNormal<'T>
        val private serializer: IMessageSerializer
        //val private context: obj option
        val private memoizedValue : Lazy<'T> 

        new (value: 'T) = { 
            rawValue = NormalMessage value 
            serializer = Unchecked.defaultof<IMessageSerializer>
            memoizedValue = Unchecked.defaultof<Lazy<'T>>
            //context = None
        }

        private new (info: SerializationInfo, context: StreamingContext) = 
            let raw = info.GetValue("rawValue", typeof<byte[]>) :?> byte[]
            let serializerName = info.GetString("serializerName")
            let serializer = SerializerRegistry.Resolve(serializerName)
            {
                rawValue = RawMessage raw
                serializer = serializer
                memoizedValue = lazy (serializer.Deserialize<'T>(raw))
            }

#if LOG_RAW_MSG_PAYLOAD_SIZE
        static member private Logger with get() = Nessos.MBrace.Utils.IoC.Resolve<Nessos.MBrace.Utils.IActorLogger>()
#endif
        member r.Value = r.rawValue

        member r.Normal 
            with get() = match r.rawValue with
                         | RawMessage bytes -> r.memoizedValue.Value
                         | NormalMessage value -> value

        interface ISerializable with
            member r.GetObjectData(info: SerializationInfo, context: StreamingContext) =
                match r.rawValue, context.Context with
                | NormalMessage value, (:? MessageSerializationContext as messageSerializationContext) ->
                    let raw = messageSerializationContext.Serializer.Serialize(value, context)
#if LOG_RAW_MSG_PAYLOAD_SIZE
                    Raw<_>.Logger.LogInfo <| sprintf "SERIALIZED RAW MSG. Length = %d" raw.Length
#endif
                    info.AddValue("rawValue", raw)
                    info.AddValue("serializerName", messageSerializationContext.Serializer.Name)
                | RawMessage raw, (:? MessageSerializationContext as messageSerializationContext) ->
                    info.AddValue("rawValue", raw)
                    info.AddValue("serializerName", messageSerializationContext.Serializer.Name)
                | NormalMessage value, _ -> 
                    let raw = SerializerRegistry.GetDefaultSerializer().Serialize(value, context)

                    info.AddValue("rawValue", raw)
                    info.AddValue("serializerName", SerializerRegistry.GetDefaultSerializer().Name)
                | RawMessage raw, _ ->
                    info.AddValue("rawValue", raw)
                    info.AddValue("serializerName", SerializerRegistry.GetDefaultSerializer().Name)
                    //failwith "Message not serialized through a message serialization context."

        
    
    module Raw =
        let fromMessage (msg: 'T): Raw<'T> = new Raw<'T>(msg)

        let toMessage (raw: Raw<'T>): 'T = raw.Normal

        let (|R|) (raw : Raw<'T>) : 'T = raw.Normal

    [<AutoOpen>]
    module Guarantee =
        let (|G|) (guarantee: Guarantee<'T>): 'T =
            guarantee.Payload

    module ActorRef =
        //the result is not serializable
        //use only locally
        //equality is inherited from the original ref
        let map (mapF: 'U -> 'T) (actorRef: ActorRef<'T>): ActorRef<'U> =
            let newName = sprintf "%sProxyByMapFrom%s" actorRef.Name (typeof<'U>).Name
            {
                //the mailboxprotocol is not used, but it makes sure the generated actorRef
                //cannot be serialized
                new ActorRef<'U>(actorRef.UUId, newName, [| new MailboxActorProtocol<'U>(actorRef.UUId, newName) |]) with
                    override ref.PostAsync(msg: 'U) =
                        actorRef.PostAsync(mapF msg)

                    override ref.Post(msg: 'U) =
                        actorRef <-- (mapF msg)

                    override ref.PostWithReply<'R>(msgF: IReplyChannel<'R> -> 'U): Async<'R> =
                        actorRef <!- fun ch -> mapF (msgF ch)

                    override ref.PostWithReply<'R>(msgF: IReplyChannel<'R> -> 'U, timeout: int): Async<'R> =
                        actorRef.PostWithReply<'R>((fun ch -> mapF (msgF ch)), timeout)

                    override ref.TryPostWithReply<'R>(msgF: IReplyChannel<'R> -> 'U, timeout: int): Async<'R option> =
                        actorRef.TryPostWithReply<'R>((fun ch -> mapF (msgF ch)), timeout)

            }

        let raw (actorRef: ActorRef<Raw<'T>>): ActorRef<'T> = map Raw.fromMessage actorRef

    module Actor =
        // merged from ClientPack
        let reliableBroadcast (actors: #seq<Actor<'T>>): Actor<Rely<'T>> =
            let rec broadcastBehavior (self: Actor<Rely<'T>>) =
                let recipientCount = actors |> Seq.length
                async {
                    let! (R(reply), msg) = self.Receive()

                    let exceptions =
                        actors |> Seq.fold (fun exceptions actor ->
                                            try
                                                !actor <-- msg
                                                None::exceptions
                                            with e ->
                                                (Some e)::exceptions
                                        ) []
                                |> Seq.choose id

                    if recipientCount > 0 && exceptions |> Seq.length = recipientCount then
                        reply << Exception <| (CommunicationException("Broadcast has failed. Unable to send message to any target.", exceptions |> Seq.head) :> exn)
                    else
                        //confirm broadcast
                        reply nothing

                    return! broadcastBehavior self
                }

            actors |> Seq.map (fun a -> a :> ActorBase) |> Actor.bindLinked broadcastBehavior

        let rec reliablyBroadcastMsg (msgBuilder: IReplyChannel<'R> -> 'T) (actorRefs: ActorRef<'T> list): Async<'R> =
            async {
                match actorRefs with
                | [] -> 
                    return raise <| CommunicationException("Broadcast has failed. Unable to send message to any target.")
                | actorRef::rest ->
                    try
                        let! result = actorRef <!- msgBuilder

                        return result
                    with :? CommunicationException ->
                        return! reliablyBroadcastMsg msgBuilder rest
            }

        let parallelPostWithReply (msgBuilder : IReplyChannel<'R> -> 'T) (actorRefs : ActorRef<'T> list) : Async<Reply<'R> []> =
            let post actorRef =
                async {
                    try
                        let result = actorRef <!= msgBuilder

                        return Value result
                    with e -> return Exception e
                }
                
            actorRefs |> Seq.map post |> Async.Parallel
                        

        let failoverOnResult (msgBuilderF: 'T -> ((IReplyChannel<'R> * (IReplyChannel<'R> -> 'T)) option)) (actors: #seq<Actor<'T>>): Actor<'T> =
            let rec failoverLoop post (reportException: Exception -> unit) actors =
                async {
                    match actors with
                    | actor::rest ->
                        try
                            do! post actor
                        with _ ->
                            return! failoverLoop post reportException rest
                    | [] -> 
                        reportException <| CommunicationException("All failovers have failed. Unable to send message.")
                        return ()
                }

            let rec failoverBehavior (self: Actor<'T>) =
                async {
                    let! msg = self.Receive()

                    match msgBuilderF msg with
                    | Some(R(reply), msgBuilder) ->
                        do! actors |> Seq.toList |> failoverLoop (fun actor -> async { let! result = !actor <!- msgBuilder in reply <| Value result }) (reply << Exception)
                    | None -> 
                        do! actors |> Seq.toList |> failoverLoop (fun actor -> async { !actor <-- msg }) self.LogError

                    return! failoverBehavior self
                }

            Actor.bindLinked failoverBehavior (actors |> Seq.cast<ActorBase>)
        // end clientpack merge


        //TODO!!! Remove from here; was moved to Actors.Cluster
        type private LogSubscribedActor<'T> internal (otherActor: Actor<'T>, observerF: Log -> unit) =
            inherit Actor<'T>(otherActor)

            let mutable subscriptionRef = None : IDisposable option 

            override self.Publish(protocols: IActorProtocol<'T>[]) = 
                new LogSubscribedActor<'T>(otherActor.Publish(protocols), observerF) :> Actor<'T>

            override self.Rename(newName: string) =
                new LogSubscribedActor<'T>(otherActor.Rename(newName), observerF) :> Actor<'T>

            override self.Start() =
                subscriptionRef |> Option.iter (fun d -> d.Dispose())
                subscriptionRef <- self.Log |> Observable.subscribe observerF |> Some
                base.Start()

            override self.Stop() =
                subscriptionRef |> Option.iter (fun d -> d.Dispose())
                base.Stop()


        let subscribeLog (observerF: Log -> unit) (actor: Actor<'T>): Actor<'T> = 
            new LogSubscribedActor<'T>(actor, observerF) :> Actor<'T>

//        let rec private constructBootstrapped(name: string, protocols: IActorProtocol<'T>[], behavior, linkedActors: seq<ActorBase>) =
//            {
//                new Actor<'T>(ActorUUID.Empty, name, protocols, behavior, linkedActors) with
//                    override a.Ref: ActorRef<'T> =
//                        base.Ref |> ActorRef.bootstrap
//
//                    override a.Rename(newName: string): Actor<'T> =
//                        constructBootstrapped(newName, protocols, behavior, linkedActors)
//
//                    override a.Publish(protocols': IActorProtocol<'T>[]): Actor<'T> =
//                        constructBootstrapped(name, protocols' |> Array.append protocols, behavior, linkedActors)
//            }

//        module Behavior =
//            let bootstrap (behavior: Actor<'T> -> Async<unit>) =
//                constructBootstrapped("", [| new MailboxActorProtocol<'T>(ActorUUID.Empty, "") |], behavior, Seq.empty)

//        let bootstrap (actor: Actor<'T>): Actor<'T> =
//            let rec behavior (self: Actor<'T>) = async {
//                let! msg = self.Receive()
//                !actor <-- msg
//                return! behavior self
//            }
//
//            constructBootstrapped("", [| new MailboxActorProtocol<'T>(ActorUUID.Empty, "") |], behavior, [actor])

        let mapMany (mapManyF: 'U -> seq<'T>) (actor: Actor<'T>): Actor<'U> =
            Actor.spawnLinked(fun msg -> async {
                for mappedMsg in mapManyF msg do !actor <-- mappedMsg
            }) [actor]

        let reliablePostAction (postActionF: Rely<'T> -> Async<unit>) (actor: Actor<Rely<'T>>): Actor<Rely<'T>> =
            Actor.spawnLinked(fun (confirmChannel, msg) -> async {
                do! !actor <!- fun ch -> ch, msg

                do! postActionF (confirmChannel, msg)
            }) [actor]

        let replicate (replicas: #seq<Actor<'T>>) (actor: Actor<'T>): Actor<Rely<'T>> =
            reliableBroadcast replicas
            |> reliablePostAction (fun (R(reply), msg) -> async { !actor <-- msg; reply nothing })

        let replicateSome (predicate: 'T -> bool) (replicas: #seq<Actor<'T>>) (actor: Actor<'T>): Actor<Rely<'T>> =
            reliableBroadcast replicas
            |> Actor.filter (fun (_, msg) -> predicate msg)
            |> Actor.intercept (fun (_, msg) -> async{ !actor <-- msg })

        let replicated (replicas: #seq<Actor<'T>>) (actor: Actor<'T>): Actor<Replicated<'T>> =
            (replicate replicas actor |> Actor.map (function Replicated msg -> msg | _ -> failwith "Invalid split"), 
             actor |> Actor.map (function Singular msg -> msg | _ -> failwith "Invalid split") 
            ) |> Actor.split (function Replicated _ -> true | Singular _ -> false)

        let asyncReplicated (replicated: Actor<Replicated<'T>>): Actor<AsyncReplicated<'T>> =
            Actor.spawnLinked (fun msg -> async {
                match msg with
                | AsyncReplicated payload -> do! !replicated <!- fun ch -> Replicated(ch, payload)
                | SyncReplicated(R reply, payload) -> 
                    do! !replicated <!- fun ch -> Replicated(ch, payload) 
                    reply nothing
                | SyncSingular payload -> !replicated <-- Singular payload
            }) [replicated]

        let failover (first: Actor<'T>) (second: Actor<'T>): Actor<'T> =
            failoverOnResult (fun _ -> None) [first; second]

        let tolerate (queues: #seq<Actor<Queue<'T>>>) (actor: Actor<Guarantee<'T>>): Actor<'T> =
            let peekQueues = queues |> failoverOnResult (function Peek ch -> Some(ch, Peek) | _ -> None)
            let broadcastQueues = queues |> reliableBroadcast
            let rec tolerantBehavior (self: Actor<'T>) =
                async {
                    let! msg = !peekQueues <!- Peek

                    do! !actor <!- fun ch -> new Guarantee<'T>(ch, msg)

                    do! !broadcastQueues <!- fun ch -> ch, Dequeue

                    return! tolerantBehavior self
               }

            Actor.bindLinked tolerantBehavior [actor; peekQueues; broadcastQueues]

        let buffer (bufferQueue: Actor<Queue<'T>>): Actor<'T> =
            Actor.spawnLinked(fun msg -> async {
                !bufferQueue <-- Enqueue(msg)
            }) [bufferQueue]

        let suicidal (actor: Actor<'T>): Actor<Suicidal<'T>> =
            let rec suicidalBehavior (self: Actor<Suicidal<'T>>) =
                async {
                    let! msg = self.Receive()

                    match msg with
                    | Suicidal.Msg msg ->
                        !actor <-- msg
                    | Suicide ->
                        self.Stop()

                    return! suicidalBehavior self
                }

            Actor.bindLinked suicidalBehavior [actor]

        let controllable (actor: Actor<'T>): Actor<Controllable<'T>> =
            let rec controlBehavior (self: Actor<Controllable<'T>>) =
                let rec running () = async {
                        let! msg = self.Receive()

                        match msg with
                        | Controllable.Msg msg ->
                            !actor <-- msg
                            return! running()
                        | Pause ->
                            return! paused()
                        | Controllable.Override msg ->
                            !actor <-- msg
                            return! running()
                        | _ ->
                            return! running()
                    }
                and paused () = 
                    self.Scan(
                        function Resume -> Some <| running()
                                 | Override msg ->
                                    !actor <-- msg
                                    None
                                 | _ -> None
                    )
        
                running()

            Actor.bindLinked controlBehavior [actor]

        let swapable (actor: Actor<'T>): Actor<Swapable<'T>> =
            let rec swapableBehavior (currentActor: Actor<'T>) (self: Actor<Swapable<'T>>) =
                async {
                    let! msg = self.Receive()

                    match msg with
                    | Swapable.Msg msg -> 
                        !currentActor <-- msg
                        return! swapableBehavior currentActor self
                    | Swap(R(reply), newActor) ->
                        reply nothing
                        return! swapableBehavior newActor self
                }

            Actor.bindLinked (swapableBehavior actor) [actor]

        let raw (actor: Actor<'T>): Actor<Raw<'T>> = Actor.map Raw.toMessage actor


    module Controllable =
        let unwrap (actor: Actor<Controllable<'T>>): Actor<'T> =
            Actor.spawnLinked (fun msg -> async {
                !actor <-- Controllable.Msg msg
            }) [actor]

        let unwrapOverride (actor: Actor<Controllable<'T>>): Actor<'T> =
            Actor.spawnLinked (fun msg -> async {
                !actor <-- Controllable.Override msg
            }) [actor]

    module Swapable =
        let unwrap (actor: Actor<Swapable<'T>>): Actor<'T> =
            Actor.spawnLinked (fun msg -> async {
                !actor <-- Swapable.Msg msg
            }) [actor]

    module Behavior =
        /// apply the return of each stateful loop to a side effect function
        let statefulEvent (sideEffect : 'State -> unit) (behavior : 'State -> Actor<'T> -> Async<'State>) =
            fun state self ->
                async {
                    let! state' = behavior state self

                    do sideEffect state'

                    return state'
                }

        module Stateless =
            let guarantee (behavior: 'T -> Async<unit>) (msg: Guarantee<'T>): Async<unit> =
                async {
                    use guarantee = msg
                    let (G payload) = msg

                    return! behavior payload
                }

        module stateful =
            let guarantee (behavior: 'U -> 'T -> Async<'U>) (state: 'U) (msg: Guarantee<'T>): Async<'U> =
                async {
                    use guarantee = msg
                    let (G payload) = msg

                    return! behavior state payload
                }


    // merged from ClientPack

    module Failover =
        // the failover post functions will attempt to post the provided message to exactly one actor
        // of the provided list. Will attempt sequentually and will raise an exception if all post attempts fail.
        // If error is encountered, will probe for updated actor list and return it.
        let post, postWithReply =
            let rec tryMsg refreshMsg logger firstAttempt (actors : ActorRef<'T> list) msgContainer =
                async {
                    match actors with
                    | candidate :: rest ->
                        try
                            let! reply =
                                async {
                                    match msgContainer with
                                    | Choice1Of2 msg -> candidate <-- msg ; return None
                                    | Choice2Of2 msgBuilder ->
                                        try
                                            let! res = candidate <!- msgBuilder
                                             
                                            return res |> Value |> Some
                                        with
                                        | MessageHandlingException(_,_,_,e) -> return e |> Exception |> Some
                                        | e -> return raise e // expecting communication exception
                                }

                            if firstAttempt then
                                return reply, actors
                            else
                                let! actors' = candidate <!- refreshMsg
                                return reply, Array.toList actors' 
                                                
                        with e ->
                            logger e

                            do! Async.Sleep 200

                            return! tryMsg refreshMsg logger false rest msgContainer
                                            
                    | [] -> 
                        return raise <| CommunicationException("Cannot communicate; all failovers have failed.")

            }

            (fun refreshMsg logger actors msg -> async { let! _,state' = tryMsg refreshMsg logger true actors <| Choice1Of2 msg in return state' }),
                    fun refreshMsg logger actors msgBuilder -> 
                        async { 
                            let! reply,state' = tryMsg refreshMsg logger true actors <| Choice2Of2 msgBuilder
                            return reply.Value, state'
                        }

    module RetryExtensions =

        open System.Threading

        type ActorRef<'T> with
            member self.PostRetriable(message : 'T, retries, ?waitInterval) =
                let waitInterval = defaultArg waitInterval 100
                // negative inputs mean infinite retries
                let retries = if retries < 0 then -2 else retries

                let rec retryloop =
                    function
                    | -1 ->
                        raise <| TimeoutException("Connections attempts reached maximum permitted")
                    |  n ->
                        try
                            self.Post message
                        with
                        | :? TimeoutException
                        | CommunicationException _ -> Thread.Sleep(waitInterval) ; retryloop (n-1)
                        | _ -> reraise()


                async { return retryloop retries }

            member self.PostWithReplyRetriable(msgBuilder, retries, ?waitInterval) =
                let waitInterval = defaultArg waitInterval 100
                // negative inputs mean infinite retries
                let retries = if retries < 0 then -2 else retries

                let rec retryloop n = async {
                    match n with
                    | -1 ->
                        return raise <| CommunicationException("Connections attempts reached maximum permitted.")
                    |  _ ->
                        try return! self.PostWithReply msgBuilder
                        with
                        // bug: actors do not wrap SocketException from client side
                        | :? Sockets.SocketException
                        | :? TimeoutException
                        | CommunicationException _ -> 
                            Thread.Sleep(waitInterval) 
                            return! retryloop (n-1)
                        | e -> return raise e // reraise?
                }

                retryloop retries


        [<AutoOpen>]
        module Operators =

            let internal RetryInterval = 100
            
            let (<===) (actor : ActorRef<'T>) (msg, timeout) = 
                actor.PostRetriable(msg,timeout / RetryInterval, RetryInterval)
                |> fun expr -> Async.RunSynchronously(expr, timeout)
            
            let (<!==) (actor : ActorRef<'T>) (msgBuilder, timeout) =
                actor.PostWithReplyRetriable(msgBuilder,timeout / RetryInterval,RetryInterval)
                |> fun expr -> Async.RunSynchronously(expr, timeout)


            

