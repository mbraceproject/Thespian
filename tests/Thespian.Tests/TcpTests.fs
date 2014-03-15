namespace Thespian.Tests

    open System
    open System.Diagnostics
    open System.Net
    open Thespian
    open Thespian.Serialization
    open Thespian.Remote
    open Thespian.Remote.TcpProtocol
    open Thespian.Tests.FsUnit
    open Thespian.Tests.RemoteTesting
    open NUnit.Framework

    [<TestFixture>]
    type ``Tcp Tests``() =
        [<TestFixtureSetUp>]
        member t.TestInit() =
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out)) |> ignore
            TcpListenerPool.DefaultHostname <- "localhost"
            TcpListenerPool.RegisterListener(4244, concurrentAccepts = 10)

        [<TestFixtureTearDown>]
        member t.TestFini() =
            TcpListenerPool.Clear()
            ConnectionPool.TcpConnectionPool.Fini()

        [<Test>]
        member t.``Mailbox to BTcp to UTcp foreign reply channel``() =
            use remoteStateDomain = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4242, 0, "stateDomain")
            let stateManager = remoteStateDomain.RemoteActorManager
            stateManager.PublishActor()
            stateManager.StartActor()
            use remoteForwarderDomain = new RemoteDomainManager<IntermediateActorBTcpManager<SimpleStateActor<int>>, unit>(4243, (), "forwarderDomain")
            let forwarderManager = remoteForwarderDomain.RemoteActorManager
            forwarderManager.Init(stateManager.ActorRef)
            forwarderManager.PublishActor()
            forwarderManager.StartActor()

            use localForwarder = intermediateActor forwarderManager.ActorRef |> Actor.start

            !localForwarder <-- SimpleStateSet 42

            let result = !localForwarder <!= SimpleStateGet
            result |> should equal 42

        [<Test>]
        member t.``Exclusive listener``() =
            try
                TcpListenerPool.RegisterListener(4245, concurrentAccepts = 10, exclusive = true)

                let exclusiveActor = simpleStateActor() |> Actor.publish [Bidirectional.BTcp(port = 4245)]

                let otherActors = [ for i in 1..10 -> simpleStateActor() |> Actor.publish [Unidirectional.UTcp()] ]

                otherActors |> List.forall (fun otherActor ->
                    ((otherActor.Ref.[UTCP].Configurations.[0] :?> TcpProtocolConfiguration).Addresses |> Set.ofList)
                    <>
                    ((exclusiveActor.Ref.[BTCP].Configurations.[0] :?> TcpProtocolConfiguration).Addresses |> Set.ofList)
                ) |> should equal true
            finally 
                t.TestFini()
                t.TestInit()

        [<Test; Repeat 100>]
        member __.``Post to btcp and then to utcp``() =
            use simpleStateActorBtcp = simpleStateActor() |> Actor.publish [Bidirectional.BTcp()] |> Actor.start
            use simpleStateActorUtcp = simpleStateActor() |> Actor.publish [Unidirectional.UTcp()] |> Actor.start

            simpleStateActorBtcp.Ref.[BTCP] <-- SimpleStateSet 4242
            simpleStateActorUtcp.Ref.[UTCP] <-- SimpleStateSet 42

            let resultBtcp = simpleStateActorBtcp.Ref.[BTCP] <!= SimpleStateGet
            let resultUtcp = simpleStateActorUtcp.Ref.[UTCP] <!= SimpleStateGet

            resultBtcp |> should equal 4242
            resultUtcp |> should equal 42

        [<Test; Repeat 100>]
        member __.``Post with reply to btcp and then to utcp``() =
            use confirmedStateActorBtcp = confirmedStateActor() |> Actor.publish [Bidirectional.BTcp()] |> Actor.start
            use confirmedStateActorUtcp = confirmedStateActor() |> Actor.publish [Unidirectional.UTcp()] |> Actor.start
            
            confirmedStateActorBtcp.Ref.[BTCP] <!= fun ch -> ConfirmedStateSet(ch, 4242)
            confirmedStateActorUtcp.Ref.[UTCP] <!= fun ch -> ConfirmedStateSet(ch, 42)

            let resultBtcp = confirmedStateActorBtcp.Ref.[BTCP] <!= ConfirmedStateGet
            let resultUtcp = confirmedStateActorUtcp.Ref.[UTCP] <!= ConfirmedStateGet

            resultBtcp |> should equal 4242
            resultUtcp |> should equal 42

        [<Test; Repeat 5>]
        member __.``Parallel post with reply to btcp and then to utcp``() =
            use confirmedStateActorBtcp = confirmedStateActor() |> Actor.publish [Bidirectional.BTcp()] |> Actor.start
            use confirmedStateActorUtcp = confirmedStateActor() |> Actor.publish [Unidirectional.UTcp()] |> Actor.start

            [ for i in 1..100 ->
                confirmedStateActorBtcp.Ref.[BTCP] <!- fun ch -> ConfirmedStateSet(ch, 4242)
            ] |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

            [ for i in 1..100 ->
                confirmedStateActorUtcp.Ref.[UTCP] <!- fun ch -> ConfirmedStateSet(ch, 42)
            ] |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

            let resultBtcp = confirmedStateActorBtcp.Ref.[BTCP] <!= ConfirmedStateGet
            let resultUtcp = confirmedStateActorUtcp.Ref.[UTCP] <!= ConfirmedStateGet

            resultBtcp |> should equal 4242
            resultUtcp |> should equal 42
