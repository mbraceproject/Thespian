namespace Nessos.Thespian.Tests

    open System
    open System.Net
    open System.Diagnostics
    open Nessos.Thespian
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote
    open Nessos.Thespian.Remote.TcpProtocol
    open Nessos.Thespian.Remote.TcpProtocol.Bidirectional
    open Nessos.Thespian.Tests.FsUnit
    open Nessos.Thespian.Tests.RemoteTesting
    open NUnit.Framework

    [<TestFixture>]
    type ``BTcp Tests``() =
        [<TestFixtureSetUp>]
        member t.TestInit() =
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out)) |> ignore
            TcpListenerPool.DefaultHostname <- "localhost"
            TcpListenerPool.RegisterListener(4242, concurrentAccepts = 10)
            ConnectionPool.TcpConnectionPool.Init()

        [<TestFixtureTearDown>]
        member t.TestFini() =
            TcpListenerPool.Clear()
            ConnectionPool.TcpConnectionPool.Fini()

        [<Test>]
        member t.``Publish UTcp()``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [BTcp()]

            simpleStateActor.Ref.Protocols.Length |> should equal 2

            simpleStateActor.Ref.Configurations.Length |> should equal 1


        [<Test>]
        member t.``Published actor compared unpublished actor``() =
            let simpleStateActorUnpublished = simpleStateActor()
            use simpleStateActor = simpleStateActorUnpublished |> Actor.publish [BTcp()] |> Actor.start

            simpleStateActor.UUId |> should not (equal simpleStateActorUnpublished.UUId)
            simpleStateActor.Name |> should equal simpleStateActor.Name

            !simpleStateActorUnpublished |> should not (equal !simpleStateActor)

        [<Test>]
        member t.``Post one way with operator``() =
            let refCell = ref 0
            use simpleActor = Actor.bind <| Behavior.stateless (simpleRefCellActorBehavior refCell)
                              |> Actor.publish [BTcp()]
                              |> Actor.start

            simpleActor.Ref.[BTCP] <-- SimpleOneWay 1

            //do something for a while
            System.Threading.Thread.Sleep(500)

            refCell.Value |> should equal 1

        [<Test>]
        [<ExpectedException(typeof<UnknownRecipientException>)>]
        member t.``Post one way with actor stopped``() =
            let simpleStateActor = simpleStateActor() |> Actor.publish [BTcp()]

            simpleStateActor.Ref.[BTCP] <-- SimpleStateSet 42

        [<Test>]
        [<ExpectedException(typeof<UnknownRecipientException>)>]
        member t.``Post when started, stop and post``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [BTcp()] |> Actor.start

            simpleStateActor.Ref.[BTCP] <-- SimpleStateSet 42

            let result = simpleStateActor.Ref.[BTCP] <!= SimpleStateGet

            result |> should equal 42

            simpleStateActor.Stop()

            simpleStateActor.Ref.[BTCP] <-- SimpleStateSet 0

        [<Test>]
        member t.``Post with reply operator``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [BTcp()] |> Actor.start

            simpleStateActor.Ref.[BTCP] <-- SimpleStateSet 42
            let result = simpleStateActor.Ref.[BTCP] <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        member t.``Post with reply on collocated actor through a non-collocated ref``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [BTcp()] |> Actor.start

            simpleStateActor.Ref.[BTCP] <-- SimpleStateSet 42

            let serializer = SerializerRegistry.GetDefaultSerializer()
            let serializedRef = serializer.Serialize(obj(), simpleStateActor.Ref)
            let deserializedRef = serializer.Deserialize(obj(), serializedRef) :?> ActorRef<SimpleStateActor<int>>

            let result = deserializedRef <!= SimpleStateGet
            result |> should equal 42

        [<Test>]
        member t.``Order of post from specific client is order of processing on actor.``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [BTcp()] |> Actor.start

            let simpleStateActorRef = simpleStateActor.Ref.[BTCP]

            simpleStateActorRef <-- SimpleStateSet 1
            simpleStateActorRef <-- SimpleStateSet 2
            simpleStateActorRef <-- SimpleStateSet 3
            simpleStateActorRef <-- SimpleStateSet 4
            simpleStateActorRef <-- SimpleStateSet 1
            simpleStateActorRef <-- SimpleStateSet 3
            simpleStateActorRef <-- SimpleStateSet 42
            simpleStateActorRef <-- SimpleStateSet 7777
            simpleStateActorRef <-- SimpleStateSet 42

            let result = simpleStateActorRef <!= SimpleStateGet

            result |> should equal 42

        [<Test; Repeat 1>]
        member t.``Parallel posts``() =
            use adder = adderActor() |> Actor.publish [BTcp()] |> Actor.start

            [ for i in 1..10 -> async { adder.Ref.[BTCP] <-- SimpleStateSet i } ]
            |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

            let result = adder.Ref.[BTCP] <!= SimpleStateGet

            let expectedResult = [for i in 1..10 -> i] |> List.reduce (+)

            result |> should equal expectedResult

        [<Test; Repeat 100>]
        member t.``Parallel posts with reply``() =
            use confirmedStateActor = confirmedStateActor() |> Actor.publish [BTcp()] |> Actor.start

            [for i in 1..10 -> confirmedStateActor.Ref.[BTCP] <!- fun ch -> ConfirmedStateSet(ch, i)]
            |> Async.Parallel |> Async.Ignore
            |> Async.RunSynchronously

            let result = confirmedStateActor.Ref.[BTCP] <!= ConfirmedStateGet

            result |> should be (between 1 10)

        [<Test>]
        [<ExpectedException(typeof<System.TimeoutException>)>]
        member t.``Post with reply with timeout``() =
            use notReplyingActor = notReplyingActor() |> Actor.publish [BTcp()] |> Actor.start
            
            notReplyingActor.Ref.[BTCP] <!= fun ch -> DoNotReply(ch.WithTimeout(500))

        [<Test>]
        member t.``Post with exception reply``() =
            use exceptionActor = exceptionActor() |> Actor.publish [BTcp()] |> Actor.start

            exceptionActor.Ref.[BTCP] <!= GetUnitReply

            try 
                exceptionActor.Ref.[BTCP] <!= GetExceptionReply
                failwith "No exception in reply."
            with MessageHandlingException(_, _, _, e) -> e.Message |> should equal "42"

        [<Test>]
        member t.``Post one way``() =
            let refCell = ref 0
            use simpleActor = Actor.bind <| Behavior.stateless (simpleRefCellActorBehavior refCell)
                              |> Actor.publish [BTcp()]
                              |> Actor.start

            simpleActor.Ref.[BTCP].Post(SimpleOneWay 1)

            //do something for a while
            System.Threading.Thread.Sleep(500)

            refCell.Value |> should equal 1

        [<Test>]
        member t.``Post with reply``() =
            use stateActor = simpleStateActor() |> Actor.publish [BTcp()] |> Actor.start

            stateActor.Ref.[BTCP].Post(SimpleStateSet 42)

            let result = stateActor.Ref.[BTCP].PostWithReply(SimpleStateGet) |> Async.RunSynchronously

            result |> should equal 42

            //System.Threading.Thread.Sleep 20000

        [<Test>]
        member t.``Try post with reply``() =
            use stateActor = simpleStateActor() |> Actor.publish [BTcp()] |> Actor.start

            stateActor.Ref.[BTCP].Post(SimpleStateSet 42)

            let result = stateActor.Ref.[BTCP].TryPostWithReply(SimpleStateGet, 500) |> Async.RunSynchronously
            
            result |> should equal (Some 42)

        [<Test>]
        [<ExpectedException(typeof<MessageHandlingException>)>]
        member t.``Try post with reply exception``() =
            use exceptionActor = exceptionActor() |> Actor.publish [BTcp()] |> Actor.start

            exceptionActor.Ref.[BTCP].TryPostWithReply(GetExceptionReply, 500) |> Async.RunSynchronously |> ignore

        [<Test>]
        member t.``Try post with reply timeout``() =
            use notReplyingActor = notReplyingActor() |> Actor.publish [BTcp()] |> Actor.start
            
            let result = notReplyingActor.Ref.[BTCP].TryPostWithReply(DoNotReply, 500) |> Async.RunSynchronously

            result |> should equal None

        [<Test>]
        member t.``Double publish``() =
            let simpleStateActorUnpublished = simpleStateActor()
            use publishedOne = simpleStateActorUnpublished |> Actor.publish [BTcp()] |> Actor.start
            use publishedTwo = simpleStateActorUnpublished |> Actor.publish [BTcp()] |> Actor.start

            !publishedOne |> should not (equal !publishedTwo)
            !publishedTwo |> should not (equal !simpleStateActorUnpublished)

            publishedOne.Ref.[BTCP] <-- SimpleStateSet 42
            
            let result = publishedTwo.Ref.[BTCP] <!= SimpleStateGet

            result |> should equal 0

            (fun () -> !simpleStateActorUnpublished <-- SimpleStateSet 42) |> should throw typeof<ActorInactiveException>

        [<Test>]
        [<ExpectedException(typeof<InvalidOperationException>)>]
        member t.``Double publish renamed actor``() =
            use publishedOne = simpleStateActor() |> Actor.rename "sameName" |> Actor.publish [BTcp()] |> Actor.start
            use publishedTwo = simpleStateActor() |> Actor.rename "sameName" |> Actor.publish [BTcp()] |> Actor.start

            ()

        [<Test>]
        member t.``Publish after rename``() =
            use actor = simpleStateActor() |> Actor.rename "simple" |> Actor.publish [BTcp()] |> Actor.start

            actor.Name |> should equal "simple"

        [<Test>]
        member t.``Rename after publish``() =
            use actor = simpleStateActor() |> Actor.publish [BTcp()] |> Actor.rename "simple" |> Actor.start

            actor.Name |> should equal "simple"

        [<Test>]
        member t.``Different actors with rename and publish``() =
            let unpublished = simpleStateActor()

            let renamed = unpublished |> Actor.rename "simpleStateActor"
            let published = renamed |> Actor.publish [BTcp()]

            !unpublished |> should not (equal !renamed)
            !renamed |> should not (equal !published)

            unpublished.UUId |> should not (equal renamed.UUId)
            renamed.UUId |> should not (equal published.UUId)

            unpublished.Name |> should not (equal renamed.Name)
            renamed.Name |> should equal "simpleStateActor"
            renamed.Name |> should equal published.Name

        [<Test>]
        member t.``Post to renamed actor``() =
            use actor = simpleStateActor() |> Actor.rename "simple" |> Actor.publish [BTcp()] |> Actor.start

            actor.Ref.[BTCP] <-- SimpleStateSet 42
            
            let result = actor.Ref.[BTCP] <!= SimpleStateGet

            result |> should equal 42

        [<Test; Repeat 50>]
        member t.``Post to renamed actor with separately constructed ActorRef in same appdomain``() =
            use actor = simpleStateActor() |> Actor.publish [BTcp(port = 4242)] |> Actor.rename "actorName" |> Actor.start

            let actorRef = new ActorRef<SimpleStateActor<int>>(ActorUUID.Empty, "actorName", [| new Protocol<SimpleStateActor<int>>(ActorUUID.Empty, "actorName", ClientAddress <| Address.Parse("127.0.0.1:4242")) |])

            actorRef <-- SimpleStateSet 42

            let result = actorRef <!= SimpleStateGet
            result |> should equal 42

        [<Test>]
        member t.``Post to actor in different appdomain via name id``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorBTcpManager<int>, int>(4243, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor("simpleStateActor")
            manager.StartActor()

            let actorRef = new ActorRef<SimpleStateActor<int>>(ActorUUID.Empty, "simpleStateActor", [| new Protocol<SimpleStateActor<int>>(ActorUUID.Empty, "simpleStateActor", ClientAddress <| Address.Parse("127.0.0.1:4243")) |])

            actorRef <-- SimpleStateSet 42

            let result = actorRef <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        member t.``Post to actor in different appdomain via ref with uuid``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorBTcpManager<int>, int>(4243, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()
            manager.StartActor()

            let actorRef = manager.ActorRef

            actorRef <-- SimpleStateSet 42

            let result = actorRef <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        member t.``Post to actor in different appdomain via ref from uri with name``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorBTcpManager<int>, int>(4243, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor("simpleState")
            manager.StartActor()

            let actorRef = ActorRef.fromUri "btcp://127.0.0.1:4243/*/simpleState/format.binary"

            actorRef <-- SimpleStateSet 42

            let result = actorRef <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        member t.``Post to actor in different appdomain via ref from uri with uuid``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorBTcpManager<int>, int>(4243, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()
            manager.StartActor()

            let actorUUId = manager.ActorRef.UUId

            let actorRef = ActorRef.fromUri <| sprintf "btcp://127.0.0.1:4243/%A/*/format.binary" actorUUId

            actorRef <-- SimpleStateSet 42

            let result = actorRef <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        [<ExpectedException(typeof<UnknownRecipientException>)>]
        member t.``Post to different appdomain not started actor (UnknownRecipient)``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorBTcpManager<int>, int>(4243, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()

            let actorRef = manager.ActorRef

            actorRef <-- SimpleStateSet 42

        [<Test>]
        member t.``Post to different appdomain / with wrong types (protocol Failure ack with InvalidCastException)``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorBTcpManager<int>, int>(4243, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor("simpleStateActor")
            manager.StartActor()
            //the actor is of type SimpleStateActor<int>
            //we will make an actorRef for SimpleStateActor<string>

            let actorRef = new ActorRef<SimpleStateActor<string>>(ActorUUID.Empty, "simpleStateActor", [| new Protocol<SimpleStateActor<string>>(ActorUUID.Empty, "simpleStateActor", ClientAddress <| Address.Parse("127.0.0.1:4243")) |])
            
            try
                actorRef <-- SimpleStateSet "Foobar"
                failwith "Communication exception not thrown."
            with CommunicationException(_, _, _, e) ->
                e |> should haveExactType typeof<InvalidCastException>

        [<Test>]
        [<ExpectedException(typeof<CommunicationException>)>]
        member t.``Post to different appdomain / with deserialization failure``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorBTcpManager<NonDeserializable>, NonDeserializable>(4243, new NonDeserializable(false))
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()
            manager.StartActor()
            let actorRef = manager.ActorRef

            //will serialize, but fail to deserialize
            actorRef <-- SimpleStateSet(new NonDeserializable(true))

        [<Test>]
        member t.``Post with mailbox foreign reply channel``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorBTcpManager<int>, int>(4243, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()
            manager.StartActor()
            let actorRef = manager.ActorRef

            use forwarder = intermediateActor actorRef |> Actor.start

            !forwarder <-- SimpleStateSet 42

            //here the reply channel is on the mailbox protocol of the intermediate actor
            //the intermediate actor will forward the message to the remote actor
            //then the mailbox reply channel is a foreign reply channel to the utcp protocol
            let result = !forwarder <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        member t.``Post with 2 mailbox foreign reply channels``() =
            use remoteDomain = new RemoteDomainManager<MultipleRepliesStateBTcpManager<int>, int>(4243, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()
            manager.StartActor()
            let actorRef = manager.ActorRef

            use forwarder = intermediateActor actorRef |> Actor.start
            use multipleRepliesMapper = mupltipleRepliesMap !forwarder |> Actor.start

            !multipleRepliesMapper <-- SimpleStateSet 42

            let result1 = !multipleRepliesMapper <!= SimpleStateGet
            result1 |> should equal 42

            !multipleRepliesMapper <-- SimpleStateSet 0
            let result2 = !multipleRepliesMapper <!= SimpleStateGet
            result2 |> should equal 0

        [<Test>]
        [<ExpectedException(typeof<CommunicationException>)>]
        member t.``Post to non-existing actor.``() =
            let actorRef = ActorRef.fromUri "btcp://localhost:9999/*/notExists/format.binary"

            actorRef <-- SimpleStateSet 42

        [<Test>]
        [<ExpectedException(typeof<CommunicationException>)>]
        member t.``Post with reply to non-existing actor.``() =
            let actorRef = ActorRef.fromUri "btcp://localhost:9999/*/notExists/format.binary"

            actorRef <!= SimpleStateGet |> ignore
