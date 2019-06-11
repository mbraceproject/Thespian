namespace Nessos.Thespian.Tests

open System
open System.Threading
open NUnit.Framework
open FsUnit
open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Tests.TestDefinitions

[<AbstractClass>]
type ``Collocated Communication``() = 
    let mutable defaultPrimaryProtocolFactory = Unchecked.defaultof<IPrimaryProtocolFactory>

    abstract PrimaryProtocolFactory : IPrimaryProtocolFactory
    override __.PrimaryProtocolFactory = new MailboxProtocol.MailboxPrimaryProtocolFactory() :> IPrimaryProtocolFactory

    abstract PublishActorPrimary : Actor<'T> -> Actor<'T>
    abstract RefPrimary : Actor<'T> -> ActorRef<'T>

    abstract ParallelPostsNum : int
    abstract ParallelAsyncPostsNum : int
    abstract ParallelPostsWithReplyNum : int
    
    [<OneTimeSetUp>]
    member self.SetUp() = 
        defaultPrimaryProtocolFactory <- Actor.DefaultPrimaryProtocolFactory
        Actor.DefaultPrimaryProtocolFactory <- self.PrimaryProtocolFactory
    
    [<OneTimeTearDown>]
    member self.TearDown() = Actor.DefaultPrimaryProtocolFactory <- defaultPrimaryProtocolFactory
    
    // [<TearDown>]
    // member __.TestTearDown() =
    //   let memoryUsage = GC.GetTotalMemory(true)
    //   printfn "Total Memory = %d bytes" memoryUsage

    [<Test>]
    member self.``Post method``() = 
        let cell = ref 0
        
        use actor = Actor.bind <| Behavior.stateless (Behaviors.refCell cell)
                    |> self.PublishActorPrimary
                    |> Actor.start
        self.RefPrimary(actor).Post(TestAsync 42)
        self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        cell.Value |> should equal 42
    
    [<Test>]
    member self.``Post operator``() = 
        let cell = ref 0
        
        use actor = Actor.bind <| Behavior.stateless (Behaviors.refCell cell)
                    |> self.PublishActorPrimary
                    |> Actor.start
                    
        self.RefPrimary(actor) <-- TestAsync 42
        self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        cell.Value |> should equal 42
    
    [<Test>]
    member self.``Async post method``() = 
        let cell = ref 0
        
        use actor = Actor.bind <| Behavior.stateless (Behaviors.refCell cell)
                    |> self.PublishActorPrimary
                    |> Actor.start
        Async.RunSynchronously(self.RefPrimary(actor) <-!- TestAsync 42)
        self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        cell.Value |> should equal 42
    
    [<Test>]
    member self.``Async post operator``() = 
        let cell = ref 0
        
        use actor = Actor.bind <| Behavior.stateless (Behaviors.refCell cell)
                    |> self.PublishActorPrimary
                    |> Actor.start
                    
        Async.RunSynchronously <| self.RefPrimary(actor).AsyncPost(TestAsync 42)
        self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        cell.Value |> should equal 42
    
    [<Test>]
    member self.``Untyped post method``() = 
        let cell = ref 0
        
        use actor = Actor.bind <| Behavior.stateless (Behaviors.refCell cell)
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let actorRef = self.RefPrimary(actor) :> ActorRef
        actorRef.PostUntyped(TestAsync 42 : TestMessage<int>)
        self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        cell.Value |> should equal 42
    
    [<Test>]
    member self.``Untyped async post method``() = 
        let cell = ref 0
        
        use actor = Actor.bind <| Behavior.stateless (Behaviors.refCell cell)
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let actorRef = self.RefPrimary(actor) :> ActorRef
        Async.RunSynchronously <| actorRef.AsyncPostUntyped(TestAsync 42 : TestMessage<int>)
        self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        cell.Value |> should equal 42
    
    [<Test>]
    member self.``Post with reply method``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                    |> self.PublishActorPrimary
                    |> Actor.start
        self.RefPrimary(actor).Post(TestAsync 42)
        let r = Async.RunSynchronously <| self.RefPrimary(actor).PostWithReply(fun ch -> TestSync(ch, 43))
        r |> should equal 42
    
    [<Test>]
    member self.``Post with reply operator``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                    |> self.PublishActorPrimary
                    |> Actor.start
                    
        self.RefPrimary(actor) <-- TestAsync 42
        let r = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 43)
        r |> should equal 42
    
    [<Test>]
    member self.``Post with reply method with no timeout (default timeout)``() = 
        Assert.throws<TimeoutException>(fun () ->
            use actor = Actor.bind PrimitiveBehaviors.nill
                        |> self.PublishActorPrimary
                        |> Actor.start
                    
            self.RefPrimary(actor).PostWithReply(fun ch -> TestSync(ch, ()))
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply operator with no timeout (default timeout)``() = 
        Assert.throws<TimeoutException>(fun () ->
            use actor = Actor.bind PrimitiveBehaviors.nill
                        |> self.PublishActorPrimary
                        |> Actor.start
                    
            self.RefPrimary(actor) <!= fun ch -> TestSync(ch, ()))
    
    [<Test>]
    member self.``Post with reply method with timeout (in-time)``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                    |> self.PublishActorPrimary
                    |> Actor.start
                    
        self.RefPrimary(actor) <-- TestAsync 42
        let r = self.RefPrimary(actor).PostWithReply((fun ch ->
                    TestSync(ch, Default.ReplyReceiveTimeout / 4)), 
                    Default.ReplyReceiveTimeout * 4)
                |> Async.RunSynchronously
                
        r |> should equal 42
    
    [<Test>]
    member self.``Post with reply method with timeout``() =
        Assert.throws<TimeoutException>(fun () ->
            use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                        |> self.PublishActorPrimary
                        |> Actor.start

            self.RefPrimary(actor).PostWithReply((fun ch -> TestSync(ch, Default.ReplyReceiveTimeout * 4)), Default.ReplyReceiveTimeout / 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply operator with timeout on reply channel (fluid)``() = 
        Assert.throws<TimeoutException>(fun () ->
            use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                        |> self.PublishActorPrimary
                        |> Actor.start

            self.RefPrimary(actor) <!- fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout / 4), Default.ReplyReceiveTimeout * 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply operator with timeout on reply channel (property set)``() =
        Assert.throws<TimeoutException>(fun () ->
            use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                        |> self.PublishActorPrimary
                        |> Actor.start

            self.RefPrimary(actor) <!- fun ch -> ch.Timeout <- Default.ReplyReceiveTimeout / 4; TestSync(ch, Default.ReplyReceiveTimeout * 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply method timeout (fluid) on reply channel overrides method timeout arg``() = 
        Assert.throws<TimeoutException>(fun () ->
            use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                        |> self.PublishActorPrimary
                        |> Actor.start

            self.RefPrimary(actor) <-- TestAsync 42
            //the actor will stall for Default.ReplyReceiveTimeout,
            //the reply timeout specified by the method arg is Default.ReplyReceiveTimeout * 2
            //enough to get back the reply
            //the timeout is overriden by setting the reply channel timeout to Default.ReplyReceiveTimeout/2
            //thus we expect this to timeout
            self.RefPrimary(actor).PostWithReply((fun ch -> 
                TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout / 4), Default.ReplyReceiveTimeout * 8)), 
                Default.ReplyReceiveTimeout * 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply method timeout (property set) on reply channel overrides method timeout arg``() = 
        Assert.throws<TimeoutException>(fun () ->
            use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                        |> self.PublishActorPrimary
                        |> Actor.start
                    
            self.RefPrimary(actor) <-- TestAsync 42
            self.RefPrimary(actor).PostWithReply((fun ch -> 
                ch.Timeout <- Default.ReplyReceiveTimeout / 4
                TestSync(ch, Default.ReplyReceiveTimeout * 8)), Default.ReplyReceiveTimeout * 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply sequence``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let r = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 42)
        let r' = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)

        r |> should equal 0
        r' |> should equal 42
    
    [<Test>]
    member self.``Post with reply exception response``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.divider
                    |> self.PublishActorPrimary
                    |> Actor.start

        self.RefPrimary(actor) <-- TestAsync 42

        try let r = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0) in ()
        with :? MessageHandlingException as e -> 
            let innerExceptionType = e.InnerException.GetType()
            innerExceptionType |> should equal typeof<DivideByZeroException>
            e.ActorId |> should equal (Some <| self.RefPrimary(actor).Id)
            e.ActorName |> should equal actor.Name
    
    [<Test>]
    member self.``Try post with repy exception resposne``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.divider
                    |> self.PublishActorPrimary
                    |> Actor.start
            
        self.RefPrimary(actor) <-- TestAsync 42
        try 
            self.RefPrimary(actor).TryPostWithReply(fun ch -> TestSync(ch, 0))
            |> Async.Ignore
            |> Async.RunSynchronously
        with :? MessageHandlingException as e -> 
            let innerExceptionType = e.InnerException.GetType()
            innerExceptionType |> should equal typeof<DivideByZeroException>
            e.ActorId |> should equal (Some <| self.RefPrimary(actor).Id)
            e.ActorName |> should equal actor.Name
    
    [<Test>]
    member self.``Untyped post with reply method``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let actorRef = self.RefPrimary(actor) :> ActorRef
        self.RefPrimary(actor).Post(TestAsync 42 : TestMessage<int, int>)
        let r = Async.RunSynchronously <| actorRef.PostWithReplyUntyped(fun ch -> (TestSync(ch |> ReplyChannel.map box, 43) : TestMessage<int, int>) |> box)
        
        r |> should equal 42
    
    [<Test>]
    member self.``Untyped post with reply method with timeout (in-time)``() = 
        use actor = 
            Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
            |> self.PublishActorPrimary
            |> Actor.start
        
        let actorRef = self.RefPrimary(actor) :> ActorRef
        self.RefPrimary(actor) <-- TestAsync 42
        let r = 
            actorRef.PostWithReplyUntyped
                ((fun ch -> 
                 (TestSync(ch |> ReplyChannel.map box, Default.ReplyReceiveTimeout / 4) : TestMessage<int, int>) |> box), 
                 Default.ReplyReceiveTimeout * 4) |> Async.RunSynchronously
        r |> should equal 42
    
    [<Test>]
    member self.``Untyped post with reply method with timeout``() =
        Assert.throws<TimeoutException>(fun () ->
            use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                        |> self.PublishActorPrimary
                        |> Actor.start
        
            let actorRef = self.RefPrimary(actor) :> ActorRef
            actorRef.PostWithReplyUntyped((fun ch -> 
                 (TestSync(ch |> ReplyChannel.map box, Default.ReplyReceiveTimeout * 4) : TestMessage<int, int>) |> box), 
                 Default.ReplyReceiveTimeout / 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Try post with reply``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                    |> self.PublishActorPrimary
                    |> Actor.start
                    
        self.RefPrimary(actor) <-- TestAsync 42
        let r = self.RefPrimary(actor).TryPostWithReply(fun ch -> TestSync(ch, 43)) |> Async.RunSynchronously
        r |> should equal (Some 42)
    
    [<Test>]
    member self.``Try post with reply with timeout (in time)``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                    |> self.PublishActorPrimary
                    |> Actor.start

        self.RefPrimary(actor) <-- TestAsync 42

        let r = self.RefPrimary(actor).TryPostWithReply((fun ch ->
                    TestSync(ch, Default.ReplyReceiveTimeout / 4)), 
                    Default.ReplyReceiveTimeout * 4)
                |> Async.RunSynchronously
                
        r |> should equal (Some 42)
    
    [<Test>]
    member self.``Try post with reply with timeout (time-out)``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                    |> self.PublishActorPrimary
                    |> Actor.start

        self.RefPrimary(actor) <-- TestAsync 42

        let r = self.RefPrimary(actor).TryPostWithReply((fun ch ->
                    TestSync(ch, Default.ReplyReceiveTimeout * 4)), 
                    Default.ReplyReceiveTimeout / 4)
                |> Async.RunSynchronously

        r |> should equal None
    
    [<Test>]
    member self.``Try post with reply with no timeout (default timeout)``() = 
        use actor = Actor.bind <| PrimitiveBehaviors.nill
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let r = self.RefPrimary(actor).TryPostWithReply(fun ch -> TestSync(ch, ()))
                |> Async.Ignore
                |> Async.RunSynchronously
        
        r |> should equal None
    
    [<Test>]
    member self.``Try post with reply with timeout on reply channel (fluid) overrides timeout arg``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                    |> self.PublishActorPrimary
                    |> Actor.start

        self.RefPrimary(actor) <-- TestAsync 42
        let r = self.RefPrimary(actor).TryPostWithReply((fun ch -> 
                    TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout / 4), Default.ReplyReceiveTimeout * 4)), 
                    Default.ReplyReceiveTimeout * 8)
                |> Async.RunSynchronously
                
        r |> should equal None
    
    [<Test>]
    member self.``Try post with reply with timeout on reply channel (property set) overrides timeout arg``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                    |> self.PublishActorPrimary
                    |> Actor.start

        self.RefPrimary(actor) <-- TestAsync 42
        let r = self.RefPrimary(actor).TryPostWithReply((fun ch -> 
                    ch.Timeout <- Default.ReplyReceiveTimeout / 4
                    TestSync(ch, Default.ReplyReceiveTimeout * 4)), Default.ReplyReceiveTimeout * 8)
                |> Async.RunSynchronously

        r |> should equal None
    
    [<Test>]
    member self.``Untyped try post with reply``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let actorRef = self.RefPrimary(actor) :> ActorRef
        self.RefPrimary(actor) <-- TestAsync 42
        let r = actorRef.TryPostWithReplyUntyped(fun ch -> (TestSync(ch |> ReplyChannel.map box, 43) : TestMessage<int, int>) |> box) 
                |> Async.RunSynchronously

        r |> Option.map unbox<int> |> should equal (Some 42)
    
    [<Test>]
    member self.``Untyped try post with reply with timeout (in time)``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let actorRef = self.RefPrimary(actor) :> ActorRef
        self.RefPrimary(actor) <-- TestAsync 42
        let r = actorRef.TryPostWithReplyUntyped((fun ch -> 
                    (TestSync(ch |> ReplyChannel.map box, Default.ReplyReceiveTimeout / 4) : TestMessage<int, int>) |> box), 
                    Default.ReplyReceiveTimeout * 4)
                |> Async.RunSynchronously

        r |> Option.map unbox<int> |> should equal (Some 42)
    
    [<Test>]
    member self.``Untyped try post with reply with timeout (time-out)``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let actorRef = self.RefPrimary(actor) :> ActorRef
        self.RefPrimary(actor) <-- TestAsync 42
        let r = actorRef.TryPostWithReplyUntyped((fun ch -> 
                    (TestSync(ch |> ReplyChannel.map box, Default.ReplyReceiveTimeout * 4) : TestMessage<int, int>) |> box), 
                    Default.ReplyReceiveTimeout / 4)
                |> Async.RunSynchronously

        r |> Option.map unbox<int> |> should equal None
    
    [<Test>]
    member self.``Order of posts``() = 
        use actor = Actor.bind <| Behavior.stateful [] Behaviors.list
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let msgs = 
            [ for i in 1..200 -> ListPrepend i ] @
            [ Delay 500 ] @
            [ for i in 1..200 -> ListPrepend i ]
        
        for msg in msgs do self.RefPrimary(actor) <-- msg
        let result = self.RefPrimary(actor) <!= ListGet
        
        let expected = 
            [ for i in 1..200 -> ListPrepend i ] @ [ for i in 1..200 -> ListPrepend i ]
            |> List.rev
            |> List.map (function ListPrepend i -> i | _ -> failwith "Impossibility")
        result |> should equal expected
    
    [<Test>]
    member self.``Parallel posts``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.adder
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let N = self.ParallelPostsNum

        [ for i in 1..N -> async { self.RefPrimary(actor) <-- TestAsync i } ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        
        let expected = [ for i in 1..N -> i ] |> List.reduce (+)
        
        let result = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        result |> should equal expected
    
    [<Test>]
    member self.``Parallel async posts``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.adder
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let N = self.ParallelAsyncPostsNum

        [ for i in 1..N -> self.RefPrimary(actor) <-!- TestAsync i ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        
        let expected = [ for i in 1..N -> i ] |> List.reduce (+)
        
        let result = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        result |> should equal expected
    
    [<Test>]
    member self.``Parallel posts with reply``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let N = self.ParallelPostsWithReplyNum
        [ for i in 1..N ->
              self.RefPrimary(actor) <!- fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout * 8), i) ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        
        let r = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        r |> should be (greaterThanOrEqualTo 1)
        r |> should be (lessThanOrEqualTo N)

[<AbstractClass>]
type ForeignProtocolProxy() = 
    abstract Publish : Actor<'T> -> Actor<'T>
    abstract Ref : Actor<'T> -> ActorRef<'T>

[<AbstractClass>]
type ``Collocated Remote Communication``() = 
    inherit ``Collocated Communication``()
    abstract ParallelPostsWithDeserializedNum : int
    abstract ForeignProtocols : ForeignProtocolProxy []
    
    [<Test>]
    member self.``Post to collocated actor through a non-collocated ref``() = 
        use actor = Actor.bind PrimitiveBehaviors.nill
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let serializer = Serialization.defaultSerializer
        let serializedRef = serializer.Serialize(actor.Ref)
        let deserializedRef = serializer.Deserialize<ActorRef<TestMessage<unit, unit>>>(serializedRef)
        deserializedRef <-- TestAsync()
    
    [<Test>]
    member self.``Post with reply to collocated actor through a non-collocated ref``() = 
        use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                    |> self.PublishActorPrimary
                    |> Actor.start
                    
        self.RefPrimary(actor) <-- TestAsync 42
        
        let serializer = Serialization.defaultSerializer
        let serializedRef = serializer.Serialize(actor.Ref)
        let deserializedRef = serializer.Deserialize<ActorRef<TestMessage<int, int>>>(serializedRef)
        
        let r = deserializedRef <!= fun ch -> TestSync(ch, 43)
        r |> should equal 42
    
    [<Test>]
    member self.``Publish to protocol ActorRef.Protocols/ActorRef.ProtocolFactories``() = 
        use actor = Actor.bind PrimitiveBehaviors.nill |> self.PublishActorPrimary
        actor.Ref.Protocols.Length |> should equal 2
        actor.Ref.ProtocolFactories.Length |> should equal 1
    
    [<Test>]
    member self.``Post to published stopped actor``() = 
        Assert.throws<UnknownRecipientException>(fun () ->
            use actor = Actor.bind PrimitiveBehaviors.nill |> self.PublishActorPrimary
            self.RefPrimary(actor) <-- TestAsync())
    
    [<Test>]
    member self.``Post when started, stop and post``() =
        Assert.throws<UnknownRecipientException>(fun () ->
            use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                        |> self.PublishActorPrimary
                        |> Actor.start
                    
            self.RefPrimary(actor) <-- TestAsync 42
            let r = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 43)
            r |> should equal 42
        
            actor.Stop()
            self.RefPrimary(actor) <-- TestAsync 0)
    
    [<Test>]
    member self.``Parallel posts with reply with multiple deserialised refs``() = 
        use actor : Actor<TestMessage<int, int>> = Actor.bind <| Behavior.stateful 0 Behaviors.state
                                                   |> self.PublishActorPrimary
                                                   |> Actor.start
                                                   
        self.RefPrimary(actor) <-- TestAsync 42
        
        let N = self.ParallelPostsWithDeserializedNum
        let serializer = Serialization.defaultSerializer
        let serializedRef = serializer.Serialize(self.RefPrimary(actor))
        
        let deserializedRefs = 
            [ //after 8 seconds the server resets the connection
              for i in 1..N -> //sleep more than connection timeout
                  serializer.Deserialize<ActorRef<TestMessage<int, int>>>(serializedRef) ]
        
        let result = 
            deserializedRefs
            |> Seq.map (fun ref -> ref <!- fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout * 8), 0))
            |> Async.Parallel
            |> Async.RunSynchronously
            |> Seq.reduce (+)
        
        result |> should equal 42
    
    [<Test>]
    member self.``Post with foreign reply channel`` () =
        for foreignProxy in self.ForeignProtocols do
            use nativeActor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                              |> self.PublishActorPrimary
                              |> Actor.start
        
            let nativeRef = self.RefPrimary(nativeActor)
        
            use foreignActor = Actor.bind <| Behavior.stateless (Behaviors.forward nativeRef)
                               |> foreignProxy.Publish
                               |> Actor.start
        
            let foreignRef = foreignProxy.Ref foreignActor
            foreignRef <-- TestAsync 42
            let r = foreignRef <!= fun ch -> TestSync(ch, 0)
            r |> should equal 42
    
    [<Test>]
    member self.``Post with 2 foreign reply channels`` () = 
        for foreignProxy in self.ForeignProtocols do
            use nativeActor = Actor.bind <| Behavior.stateful 0 Behaviors.multiRepliesState
                              |> self.PublishActorPrimary
                              |> Actor.start
        
            let nativeRef = self.RefPrimary(nativeActor)
        
            use forwarder = 
                Actor.bind <| Behavior.stateless (Behaviors.forward nativeRef)
                |> foreignProxy.Publish
                |> Actor.start
        
            let forwarderRef = foreignProxy.Ref forwarder
        
            use proxy = 
                Actor.bind <| Behavior.stateless (Behaviors.multiRepliesForward forwarderRef)
                |> foreignProxy.Publish
                |> Actor.start
        
            let proxyRef = foreignProxy.Ref proxy
            proxyRef <-- TestAsync 42
            let r = proxyRef <!= fun ch -> TestSync(ch, 0)
            r |> should equal 42
    
    [<Test>]
    member self.``ActorRef serialization/deserialization``() = 
        use actor = Actor.bind PrimitiveBehaviors.nill
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let actorRef = actor.Ref
        let serializer = Serialization.defaultSerializer
        let serializedRef = serializer.Serialize actorRef
        let deserializedRef = serializer.Deserialize<ActorRef<TestMessage<unit>>>(serializedRef)
        actorRef |> should equal deserializedRef
    
    [<Test>]
    member self.``ActorRef GetUris``() = 
        use actor = Actor.bind PrimitiveBehaviors.nill
                    |> self.PublishActorPrimary
                    |> Actor.start

        actor.Ref.GetUris() |> List.length |> should equal 1
    
    [<Test>]
    member self.``ActorRef toUris``() = 
        use actor = Actor.bind PrimitiveBehaviors.nill
                    |> self.PublishActorPrimary
                    |> Actor.start
                    
        !actor |> ActorRef.toUris |> List.length |> should equal 1
        actor.Ref.GetUris() |> should equal (ActorRef.toUris !actor)
    
    [<Test>]
    member self.``ActorRef toUri <-> fromUri``() = 
        use actor = Actor.bind PrimitiveBehaviors.nill
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let uri = ActorRef.toUri actor.Ref
        let actorRef = ActorRef.fromUri uri
        actorRef |> ActorRef.toUri |> should equal uri
        actorRef = actor.Ref |> should equal true

    [<Test>]
    member self.``ActorRef uri case insensitive``() =
        use actor = Actor.bind PrimitiveBehaviors.nill
                    |> self.PublishActorPrimary
                    |> Actor.start
        
        let uri = new Uri(ActorRef.toUri actor.Ref)
        let newUri =
            let ub = new UriBuilder(uri.Scheme, uri.Host.ToUpper(), uri.Port, uri.PathAndQuery)
            ub.Uri.ToString()
        let actorRef = ActorRef.fromUri newUri

        actorRef = actor.Ref |> should equal true

    [<Test>]
    member self.``ActorRef.fromUri name``() =
        use actor = Actor.bind PrimitiveBehaviors.nill
                    |> self.PublishActorPrimary
                    |> Actor.start

        let uri = ActorRef.toUri actor.Ref
        let actorRef = ActorRef.fromUri uri

        actor.Ref.Name |> should equal actorRef.Name

    [<Test>]
    member self.``Post non-serializable``() =
        use actor = Actor.bind PrimitiveBehaviors.stateless
                    |> self.PublishActorPrimary
                    |> Actor.start

        let actorRef = self.RefPrimary(actor)

        let nonSerializable = new ControlledSerializable(FailOnSerialize)

        try actorRef <-- TestAsync nonSerializable
        with :? ThespianSerializationException as e ->
            e.SerializationOperation |> should equal SerializationOperation.Serialization


    [<Test>]
    member self.``Post non-deserializable``() =
        use actor = Actor.bind PrimitiveBehaviors.stateless
                    |> self.PublishActorPrimary
                    |> Actor.start

        let actorRef = self.RefPrimary(actor)

        let nonSerializable = new ControlledSerializable(FailOnDeserialize NeverFail)

        try actorRef <-- TestAsync nonSerializable
        with DeliveryException(_, (:? ThespianSerializationException as e)) ->
            e.SerializationOperation |> should equal SerializationOperation.Deserialization
            e.InnerException.GetType() |> should not' (equal typeof<ThespianSerializationException>)

    [<Test>]
    member self.``Post non-deserializable with non-serializable failure``() =
        use actor = Actor.bind PrimitiveBehaviors.stateless
                    |> self.PublishActorPrimary
                    |> Actor.start

        let actorRef = self.RefPrimary(actor)

        let nonSerializable = new ControlledSerializable(FailOnDeserialize FailOnSerialize)

        try actorRef <-- TestAsync nonSerializable
        with DeliveryException(_, (:? ThespianSerializationException as e)) ->
            e.SerializationOperation |> should equal SerializationOperation.Deserialization
            let inner = e.InnerException :?> ThespianSerializationException
            inner.SerializationOperation |> should equal SerializationOperation.Serialization
            inner.InnerException |> should equal Unchecked.defaultof<Exception>

    [<Test>]
    member self.``Post non-deserializable with non-deserializable failure``() =
        use actor = Actor.bind PrimitiveBehaviors.stateless
                    |> self.PublishActorPrimary
                    |> Actor.start

        let actorRef = self.RefPrimary(actor)

        let nonSerializable = new ControlledSerializable(FailOnDeserialize (FailOnDeserialize NeverFail))

        try actorRef <-- TestAsync nonSerializable
        with CommunicationException(_, (:? ThespianSerializationException as e)) ->
            e.SerializationOperation |> should equal SerializationOperation.Deserialization

open Nessos.Thespian.Remote

[<AbstractClass>]
type ``Tcp communication``() = 
    inherit ``Collocated Remote Communication``()
    abstract PublishActorNonExistingListener : Actor<'T> -> Actor<'T>
    
    [<Test>]
    member self.``Publish protocol on non-existing listener``() = 
        Assert.throws<TcpProtocolConfigurationException>(fun () ->
            use actor = Actor.bind PrimitiveBehaviors.nill |> self.PublishActorNonExistingListener
            ())
    
    [<Test>]
    member self.``Posts with server connection timeout``() = 
        use actor = 
            Actor.bind <| Behavior.stateful 0 Behaviors.state
            |> self.PublishActorPrimary
            |> Actor.start
        self.RefPrimary(actor) <-- TestAsync 42
        System.Threading.Thread.Sleep(TimeSpan.FromSeconds(10.0))
        let r = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 43)
        r |> should equal 42
    
    [<Test>]
    member self.``Parallel async posts with server connection timeouts``() = 
        use actor = 
            Actor.bind <| Behavior.stateful 0 Behaviors.adder
            |> self.PublishActorPrimary
            |> Actor.start
        [ for i in 1..100 -> 
              async { 
                  if i = 21 || i = 42 || i = 84 then 
                      do! Async.Sleep 10000
                  do! self.RefPrimary(actor) <-!- (TestAsync i)
              } ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        let expected = 
            [ for i in 1..100 -> i ]
            |> List.reduce (+)
        
        let result = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        result |> should equal expected
    
    [<Test>]
    member self.``Parallel posts with reply with server connection timeout``() = 
        use actor = 
            Actor.bind <| Behavior.stateful 0 Behaviors.state
            |> self.PublishActorPrimary
            |> Actor.start
        [ for i in 1..100 -> 
              async { 
                  if i = 21 || i = 42 || i = 84 then do! Async.Sleep 10000
                  return! self.RefPrimary(actor) <!- fun ch -> TestSync(ch, i)
              } ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        let r = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 0)
        r |> should be (greaterThanOrEqualTo 1)
        r |> should be (lessThanOrEqualTo 100)
