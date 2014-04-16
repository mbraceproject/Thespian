namespace Nessos.Thespian.Tests

    open System
    open System.Net
    open System.Diagnostics
    open Nessos.Thespian
    open Nessos.Thespian.AsyncExtensions
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote
    open Nessos.Thespian.Remote.TcpProtocol
    open Nessos.Thespian.Remote.TcpProtocol.Unidirectional
    open Nessos.Thespian.Tests.FsUnit
    open Nessos.Thespian.Tests.RemoteTesting
    open NUnit.Framework

    [<TestFixture>]
    type ``UTcp Tests``() =
        [<TestFixtureSetUp>]
        member t.TestInit() =
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out)) |> ignore
            TcpListenerPool.DefaultHostname <- "localhost"
            //TcpListenerPool.RegisterListener(4242, concurrentAccepts = 10)
            TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, 4243), concurrentAccepts = 10)
            TcpListenerPool.RegisterListener(4040, concurrentAccepts = 10)
            ConnectionPool.TcpConnectionPool.Init()

        [<TestFixtureTearDown>]
        member t.TestFini() =
            TcpListenerPool.Clear()
            ConnectionPool.TcpConnectionPool.Fini()

        [<Test>]
        member t.``Publish UTcp()``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [UTcp()]

            simpleStateActor.Ref.Protocols.Length |> should equal 3

            simpleStateActor.Ref.Configurations.Length |> should equal 2

        [<Test>]
        [<ExpectedException(typeof<TcpProtocolConfigurationException>)>]
        member __.``Publish UTcp() on nonexisting listener``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [UTcp(Publish.endPoints [IPEndPoint.anyIp 3838])]

            true |> should equal true


        [<Test>]
        member t.``Published actor compared unpublished actor``() =
            let simpleStateActorUnpublished = simpleStateActor()
            use simpleStateActor = simpleStateActorUnpublished |> Actor.publish [UTcp()] |> Actor.start

            simpleStateActor.UUId |> should not (equal simpleStateActorUnpublished.UUId)
            simpleStateActor.Name |> should equal simpleStateActor.Name

            !simpleStateActorUnpublished |> should not (equal !simpleStateActor)

        [<Test>]
        member t.``Post one way with operator``() =
            let refCell = ref 0
            use simpleActor = Actor.bind <| Behavior.stateless (simpleRefCellActorBehavior refCell)
                              |> Actor.publish [UTcp()]
                              |> Actor.start

            simpleActor.Ref.[UTCP] <-- SimpleOneWay 1

            //do something for a while
            System.Threading.Thread.Sleep(500)

            refCell.Value |> should equal 1

        [<Test>]
        [<ExpectedException(typeof<UnknownRecipientException>)>]
        member t.``Post one way with actor stopped``() =
            let simpleStateActor = simpleStateActor() |> Actor.publish [UTcp()]

            simpleStateActor.Ref.[UTCP] <-- SimpleStateSet 42

        [<Test>]
        [<ExpectedException(typeof<UnknownRecipientException>)>]
        member t.``Post when started, stop and post``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [UTcp()] |> Actor.start

            simpleStateActor.Ref.[UTCP] <-- SimpleStateSet 42

            let result = simpleStateActor.Ref.[UTCP] <!= SimpleStateGet

            result |> should equal 42

            simpleStateActor.Stop()

            simpleStateActor.Ref.[UTCP] <-- SimpleStateSet 0

        [<Test>]
        member t.``Post with reply operator``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [UTcp()] |> Actor.start

            simpleStateActor.Ref.[UTCP] <-- SimpleStateSet 42
            let result = simpleStateActor.Ref.[UTCP] <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        member t.``Post with reply on collocated actor through a non-collocated ref``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [UTcp()] |> Actor.start

            simpleStateActor.Ref.[UTCP] <-- SimpleStateSet 42

            let serializer = SerializerRegistry.GetDefaultSerializer()
            let serializedRef = serializer.Serialize(obj(), simpleStateActor.Ref)
            let deserializedRef = serializer.Deserialize(obj(), serializedRef) :?> ActorRef<SimpleStateActor<int>>

            let result = deserializedRef <!= SimpleStateGet
            result |> should equal 42

        [<Test>]
        member t.``Parallel post with reply with multiple deserialized refs``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [UTcp()] |> Actor.start

            simpleStateActor.Ref <-- SimpleStateSet 42

            let serializer = SerializerRegistry.GetDefaultSerializer()
            let serializedRef = serializer.Serialize(obj(), simpleStateActor.Ref)
            let deserializedRefs = [for i in 1..10 -> serializer.Deserialize(obj(), serializedRef) :?> ActorRef<SimpleStateActor<int>>]

            let results =
                deserializedRefs
                |> Seq.map (fun ref -> ref <!- SimpleStateGet)
                |> Async.Parallel
                |> Async.RunSynchronously
                |> Seq.reduce (+)

            results |> should equal (42*10)

        [<Test>]
        member t.``Parallel post with reply with multiple deserialized refs from different appdomains``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor("simpleState")
            manager.StartActor()

            let simpleStateActor = manager.ActorRef

            simpleStateActor <-- SimpleStateSet 42

            let refs = [for i in 1..2 -> manager.ActorRef]

            let results =
                refs
                |> Seq.map (fun ref -> ref <!- SimpleStateGet)
                |> Async.Parallel
                |> Async.RunSynchronously
                |> Seq.reduce (+)

            results |> should equal (42*2)

        [<Test>]
        member t.``Parallel post with reply on collocated and non-collocated refs``() =
            use collocated = simpleStateActor() |> Actor.publish [UTcp()] |> Actor.start
            use remoteDomain1 = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
            let manager1 = remoteDomain1.RemoteActorManager
            manager1.PublishActor("foo")
            manager1.StartActor()
            use remoteDomain2 = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4254, 0)
            let manager2 = remoteDomain2.RemoteActorManager
            manager2.PublishActor("bar")
            manager2.StartActor()

            collocated.Ref <-- SimpleStateSet 36
            manager1.ActorRef <-- SimpleStateSet 2
            manager2.ActorRef <-- SimpleStateSet 40

            let refList = [collocated.Ref; manager1.ActorRef; manager2.ActorRef]
            let results =
                refList |> Seq.map (fun actorRef -> async {
                    let! r = actorRef <!- SimpleStateGet

                    return r + 2
                }) |> Async.Parallel
                   |> Async.RunSynchronously
               
            results |> should equal [| 38; 4; 42 |]

        //[<Test; Repeat 100>]
        [<Test; Repeat 10>]
        member t.``Order of post from specific client is order of processing on actor.``() =
            use simpleStateActor = simpleStateActor() |> Actor.publish [UTcp()] |> Actor.start

            let simpleStateActorRef = simpleStateActor.Ref.[UTCP]

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

        //[<Test; Repeat 100>]
        [<Test; Repeat 10>]
        member t.``Order of posts.``() =
            use listActor = orderOfPostsActor [] |> Actor.publish [UTcp()] |> Actor.start

            let listActorRef = listActor.Ref.[UTCP]

            let msgs =
                [ for i in 1..200 -> ListPrepend i]
                @
                [ Delay 500]
                @
                [ for i in 1..200 -> ListPrepend i]

            for msg in msgs do listActorRef <-- msg

            let result = listActorRef <!= GetList

            let expected = 
                [ for i in 1..200 -> ListPrepend i]
                @
                [ for i in 1..200 -> ListPrepend i]
                |> List.rev
                |> List.map (function ListPrepend i -> i | _ -> failwith "FU")

            result |> should equal expected

        //[<Test; Repeat 100>]
        [<Test; Repeat 10>]
        member t.``Parallel sync posts``() =
            use adder = adderActor() |> Actor.publish [UTcp()] |> Actor.start

            [ for i in 1..30 -> async { adder.Ref.[UTCP] <-- SimpleStateSet i } ]
            |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

            let result = adder.Ref.[UTCP] <!= SimpleStateGet

            let expectedResult = [for i in 1..30 -> i] |> List.reduce (+)

            result |> should equal expectedResult

        //[<Test; Repeat 100>]
        [<Test; Repeat 10>]
        member t.``Parallel async posts``() =
            use adder = adderActor() |> Actor.publish [UTcp()] |> Actor.start

            [ for i in 1..100 -> adder.Ref.[UTCP].PostAsync (SimpleStateSet i) ]
            |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

            let result = adder.Ref.[UTCP] <!= SimpleStateGet

            let expectedResult = [for i in 1..100 -> i] |> List.reduce (+)

            result |> should equal expectedResult

        //[<Test; Repeat 50>]
        [<Test; Repeat 5>]
        member t.``Parallel async posts with server connection timeout``() =
            use adder = adderActor() |> Actor.publish [UTcp()] |> Actor.start

            [ for i in 1..100 -> async {
                if i = 21 || i = 42 || i = 84 then 
                    //sleep more than connection timeout
                    do! Async.Sleep 8000
                do! adder.Ref.[UTCP].PostAsync (SimpleStateSet i) 
            }]
            |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

            let result = adder.Ref.[UTCP] <!= SimpleStateGet

            let expectedResult = [for i in 1..100 -> i] |> List.reduce (+)

            result |> should equal expectedResult

        //[<Test; Repeat 100>]
        [<Test; Repeat 10>]            
        member t.``Parallel posts with reply``() =
            use confirmedStateActor = confirmedStateActor() |> Actor.publish [UTcp()] |> Actor.start

            [for i in 1..100 -> confirmedStateActor.Ref.[UTCP] <!- fun ch -> ConfirmedStateSet(ch, i)]
            |> Async.Parallel |> Async.Ignore
            |> Async.RunSynchronously

            let result = confirmedStateActor.Ref.[UTCP] <!= ConfirmedStateGet

            result |> should be (between 1 100)

        //[<Test; Repeat 50>]
        [<Test; Repeat 5>]
        member t.``Parallel posts with reply with server connection timeout``() =
            use confirmedStateActor = confirmedStateActor() |> Actor.publish [UTcp()] |> Actor.start

            [for i in 1..100 -> async {
                if i = 21 || i = 42 || i = 84 then 
                    //sleep more than connection timeout
                    do! Async.Sleep 8000
                do! confirmedStateActor.Ref.[UTCP] <!- fun ch -> ConfirmedStateSet(ch, i)
            }]
            |> Async.Parallel |> Async.Ignore
            |> Async.RunSynchronously

            let result = confirmedStateActor.Ref.[UTCP] <!= ConfirmedStateGet

            result |> should be (between 1 100)

        [<Test>]
        [<ExpectedException(typeof<System.TimeoutException>)>]
        member t.``Post with reply with timeout``() =
            use notReplyingActor = notReplyingActor() |> Actor.publish [UTcp()] |> Actor.start
            
            notReplyingActor.Ref.[UTCP] <!= fun ch -> DoNotReply(ch.WithTimeout(500))

        [<Test>]
        member t.``Post with exception reply``() =
            use exceptionActor = exceptionActor() |> Actor.publish [UTcp()] |> Actor.start

            exceptionActor.Ref.[UTCP] <!= GetUnitReply

            try 
                exceptionActor.Ref.[UTCP] <!= GetExceptionReply
                failwith "No exception in reply."
            with MessageHandlingException(_, _, _, e) -> e.Message |> should equal "42"

        [<Test>]
        member t.``Post one way``() =
            let refCell = ref 0
            use simpleActor = Actor.bind <| Behavior.stateless (simpleRefCellActorBehavior refCell)
                              |> Actor.publish [UTcp()]
                              |> Actor.start

            simpleActor.Ref.[UTCP].Post(SimpleOneWay 1)

            //sleep more that connection timeout here
            System.Threading.Thread.Sleep(6000)

            simpleActor.Ref.[UTCP].Post(SimpleOneWay 1)

            //do something for a while
            System.Threading.Thread.Sleep(500)

            refCell.Value |> should equal 1

        [<Test>]
        member t.``Post with reply``() =
            use stateActor = simpleStateActor() |> Actor.publish [UTcp()] |> Actor.start

            stateActor.Ref.[UTCP].Post(SimpleStateSet 42)

            let result = stateActor.Ref.[UTCP].PostWithReply(SimpleStateGet) |> Async.RunSynchronously

            result |> should equal 42

        [<Test>]
        member t.``Try post with reply``() =
            use stateActor = simpleStateActor() |> Actor.publish [UTcp()] |> Actor.start

            stateActor.Ref.[UTCP].Post(SimpleStateSet 42)

            let result = stateActor.Ref.[UTCP].TryPostWithReply(SimpleStateGet, 500) |> Async.RunSynchronously
            
            result |> should equal (Some 42)

        [<Test>]
        [<ExpectedException(typeof<MessageHandlingException>)>]
        member t.``Try post with reply exception``() =
            use exceptionActor = exceptionActor() |> Actor.publish [UTcp()] |> Actor.start

            exceptionActor.Ref.[UTCP].TryPostWithReply(GetExceptionReply, 500) |> Async.RunSynchronously |> ignore

        [<Test>]
        member t.``Try post with reply timeout``() =
            use notReplyingActor = notReplyingActor() |> Actor.publish [UTcp()] |> Actor.start
            
            let result = notReplyingActor.Ref.[UTCP].TryPostWithReply(DoNotReply, 500) |> Async.RunSynchronously

            result |> should equal None

        [<Test>]
        member t.``Double publish``() =
            let simpleStateActorUnpublished = simpleStateActor()
            use publishedOne = simpleStateActorUnpublished |> Actor.publish [UTcp()] |> Actor.start
            use publishedTwo = simpleStateActorUnpublished |> Actor.publish [UTcp()] |> Actor.start

            !publishedOne |> should not (equal !publishedTwo)
            !publishedTwo |> should not (equal !simpleStateActorUnpublished)

            publishedOne.Ref.[UTCP] <-- SimpleStateSet 42
            
            let result = publishedTwo.Ref.[UTCP] <!= SimpleStateGet

            result |> should equal 0

            (fun () -> !simpleStateActorUnpublished <-- SimpleStateSet 42) |> should throw typeof<ActorInactiveException>

        [<Test>]
        [<ExpectedException(typeof<InvalidOperationException>)>]
        member t.``Double publish renamed actor``() =
            use publishedOne = simpleStateActor() |> Actor.rename "sameName" |> Actor.publish [UTcp()] |> Actor.start
            use publishedTwo = simpleStateActor() |> Actor.rename "sameName" |> Actor.publish [UTcp()] |> Actor.start

            ()

        [<Test>]
        member t.``Publish after rename``() =
            use actor = simpleStateActor() |> Actor.rename "simple" |> Actor.publish [UTcp()] |> Actor.start

            actor.Name |> should equal "simple"

        [<Test>]
        member t.``Rename after publish``() =
            use actor = simpleStateActor() |> Actor.publish [UTcp()] |> Actor.rename "simple" |> Actor.start

            actor.Name |> should equal "simple"

        [<Test>]
        member t.``Different actors with rename and publish``() =
            let unpublished = simpleStateActor()

            let renamed = unpublished |> Actor.rename "simpleStateActor"
            let published = renamed |> Actor.publish [UTcp()]

            !unpublished |> should not (equal !renamed)
            !renamed |> should not (equal !published)

            unpublished.UUId |> should not (equal renamed.UUId)
            renamed.UUId |> should not (equal published.UUId)

            unpublished.Name |> should not (equal renamed.Name)
            renamed.Name |> should equal "simpleStateActor"
            renamed.Name |> should equal published.Name

        [<Test>]
        member t.``Post to renamed actor``() =
            use actor = simpleStateActor() |> Actor.rename "simple" |> Actor.publish [UTcp()] |> Actor.start

            actor.Ref.[UTCP] <-- SimpleStateSet 42
            
            let result = actor.Ref.[UTCP] <!= SimpleStateGet

            result |> should equal 42

        //[<Test; Repeat 50>]
        [<Test; Repeat 5>]
        member t.``Post to renamed actor with separately constructed ActorRef in same appdomain``() =
            use actor = simpleStateActor() |> Actor.publish [UTcp(port = 4243)] |> Actor.rename "actorName" |> Actor.start

            let actorRef = new ActorRef<SimpleStateActor<int>>(ActorUUID.Empty, "actorName", [| new Protocol<SimpleStateActor<int>>(ActorUUID.Empty, "actorName", ClientAddress <| Address.Parse("localhost:4243")) |])

            actorRef <-- SimpleStateSet 42

            actorRef <!= SimpleStateGet |> should equal 42

        [<Test>]
        member t.``Post to actor in different appdomain via name id``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
            let manager = remoteDomain.RemoteActorManager
            printfn "TEEEST %A" manager
            manager.PublishActor("simpleStateActor")
            manager.StartActor()

            let actorRef = new ActorRef<SimpleStateActor<int>>(ActorUUID.Empty, "simpleStateActor", [| new Protocol<SimpleStateActor<int>>(ActorUUID.Empty, "simpleStateActor", ClientAddress <| Address.Parse("localhost:4253")) |])

            actorRef <-- SimpleStateSet 42

            let result = actorRef <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        member t.``Post to actor in different appdomain via ref with uuid``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()
            manager.StartActor()

            let actorRef = manager.ActorRef

            actorRef <-- SimpleStateSet 42

            let result = actorRef <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        member t.``Post to actor in different appdomain via ref from uri with name``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor("simpleState")
            manager.StartActor()

            let actorRef = ActorRef.fromUri "utcp://localhost:4253/*/simpleState/FsPickler"

            actorRef <-- SimpleStateSet 42

            let result = actorRef <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        member t.``Post to actor in different appdomain via ref from uri with uuid``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()
            manager.StartActor()

            let actorUUId = manager.ActorRef.UUId

            let actorRef = ActorRef.fromUri <| sprintf "utcp://localhost:4253/%A/*/FsPickler" actorUUId

            actorRef <-- SimpleStateSet 42

            let result = actorRef <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        [<ExpectedException(typeof<UnknownRecipientException>)>]
        member t.``Post to different appdomain not started actor (UnknownRecipient)``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()

            let actorRef = manager.ActorRef

            actorRef <-- SimpleStateSet 42

        [<Test>]
        member t.``Post to different appdomain / with wrong types (protocol Failure ack with InvalidCastException)``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor("simpleStateActor")
            manager.StartActor()
            //the actor is of type SimpleStateActor<int>
            //we will make an actorRef for SimpleStateActor<string>

            let actorRef = new ActorRef<SimpleStateActor<string>>(ActorUUID.Empty, "simpleStateActor", [| new Protocol<SimpleStateActor<string>>(ActorUUID.Empty, "simpleStateActor", ClientAddress <| Address.Parse("127.0.0.1:4253")) |])
            
            try
                actorRef <-- SimpleStateSet "Foobar"
                failwith "Communication exception not thrown."
            with CommunicationException(_, _, _, e) ->
                e |> should haveExactType typeof<InvalidCastException>

        [<Test>]
        [<ExpectedException(typeof<CommunicationException>)>]
        member t.``Post to different appdomain / with deserialization failure``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<NonDeserializable>, NonDeserializable>(4253, new NonDeserializable(false))
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()
            manager.StartActor()
            let actorRef = manager.ActorRef

            //will serialize, but fail to deserialize
            actorRef <-- SimpleStateSet(new NonDeserializable(true))

        [<Test>]
        member t.``Post with mailbox foreign reply channel``() =
            use remoteDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
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
            use remoteDomain = new RemoteDomainManager<MultipleRepliesStateUTcpManager<int>, int>(4253, 0)
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
        member t.``Post with reply to different remote actors with same name``() =
            use remoteDomain1 = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253, 0)
            use remoteDomain2 = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4254, 0)

            let manager1 = remoteDomain1.RemoteActorManager
            manager1.PublishActor("simpleStateActor")
            manager1.StartActor()
            let manager2 = remoteDomain2.RemoteActorManager
            manager2.PublishActor("simpleStateActor")
            manager2.StartActor()

            let actorRef1 = manager1.ActorRef
            let actorRef2 = manager2.ActorRef

            actorRef1 |> should not (equal actorRef2)

            actorRef1 <-- SimpleStateSet 40
            actorRef2 <-- SimpleStateSet 2

            [ actorRef1 <!- SimpleStateGet; actorRef2 <!- SimpleStateGet]
            |> Async.Parallel
            |> Async.RunSynchronously |> Array.reduce (+) |> should equal 42

        [<Test>]
        member t.``Parallel post with reply to different appdomains``() =
            let parallelCount = 5
            
            let remoteDomains = [for i in 1..parallelCount -> new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4253+i, 0)]
            let actorRefs = remoteDomains |> List.map (fun remoteDomain -> 
                let manager = remoteDomain.RemoteActorManager in manager.PublishActor(); manager.StartActor(); manager.ActorRef
            )

            List.zip actorRefs [for i in 1..parallelCount -> i]
            |> List.iter (fun (actorRef, i) -> actorRef <-- SimpleStateSet i)

            Console.WriteLine "Set done."
            
            let results = 
                actorRefs |> List.map (fun actorRef -> actorRef <!- SimpleStateGet)
                |> Async.Parallel
                |> Async.RunSynchronously

            results |> Array.reduce (+) |> should equal ([for i in 1..parallelCount -> i] |> List.reduce (+))

            remoteDomains |> List.iter (fun remoteDomain -> use r = remoteDomain in ())

        [<Test>]
        member t.``Post to published receiver``() =
            use receiver = Receiver.create() |> Receiver.publish [UTcp()] |> Receiver.start

            let awaitResult = receiver |> Receiver.toObservable |> Async.AwaitObservable

            receiver.Ref.[UTCP] <-- 42
            awaitResult |> Async.RunSynchronously |> should equal 42

        [<Test>]
        member t.``Post to published renamed receiver``() =
            use receiver = Receiver.create() |> Receiver.rename "receiver" |> Receiver.publish [UTcp()] |> Receiver.start

            let awaitResult = receiver |> Receiver.toObservable |> Async.AwaitObservable

            receiver.Ref.[UTCP] <-- 42
            awaitResult |> Async.RunSynchronously |> should equal 42

        [<Test>]
        member t.``Post to published renamed receiver via separate ActorRef``() =
            use receiver = Receiver.create() |> Receiver.rename "receiver" |> Receiver.publish [UTcp()] |> Receiver.start

            let awaitResult = receiver |> Receiver.toObservable |> Async.AwaitObservable

            let actorRef = ActorRef.fromUri "utcp://localhost:4243/*/receiver/FsPickler"

            actorRef <-- 42
            awaitResult |> Async.RunSynchronously |> should equal 42

        [<Test>]
        [<ExpectedException(typeof<UnknownRecipientException>)>]
        member t.``Remote actor that fails``() =
            use remoteDomain = new RemoteDomainManager<FailingActorUTcpManager, unit>(4253, ())

            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()

            let actorRef = manager.ActorRef

            actorRef <-- 42

            actorRef <-- 0

        [<Test>]
        [<ExpectedException(typeof<CommunicationException>)>]
        member t.``Post with reply fail on waiting for reply``() =
            let remoteDomain = new RemoteDomainManager<AppDomainFailingActorUTcpManager2, unit>(4253, ())
            
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor()
            manager.StartActor()

            let actorUUId = manager.ActorRef.UUId

            let actorRef = ActorRef.fromUri <| sprintf "utcp://localhost:4253/%A/*/FsPickler" actorUUId

            actorRef <-- SimpleStateSet 42

            actorRef <!= SimpleStateGet |> ignore


        [<Test>]
        [<ExpectedException(typeof<CommunicationException>)>]
        member t.``Post to non-existing actor.``() =
            let actorRef = ActorRef.fromUri "utcp://localhost:9999/*/notExists/FsPickler"

            actorRef <-- SimpleStateSet 42

        [<Test>]
        [<ExpectedException(typeof<CommunicationException>)>]
        member t.``Post with reply to non-existing actor.``() =
            let actorRef = ActorRef.fromUri "utcp://localhost:9999/*/notExists/FsPickler"

            actorRef <!= SimpleStateGet |> ignore
