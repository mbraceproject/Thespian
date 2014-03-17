namespace Nessos.Thespian.Tests

    open System
    open System.Net
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
    type ``Connection Pool Tests``() =
        [<TestFixtureSetUp>]
        member t.TestInit() =
            TcpListenerPool.DefaultHostname <- "localhost"
            TcpListenerPool.RegisterListener(4242, concurrentAccepts = 10)
            ConnectionPool.TcpConnectionPool.Init(0, 1, 500)

        [<TestFixtureTearDown>]
        member t.TestFini() =
            TcpListenerPool.Clear()
            ConnectionPool.TcpConnectionPool.Fini()

        [<Test>]
        member __.``Connectivity after Connection Refused``() =
            let remoteDomain = new RemoteDomainManager<AppDomainFailingActorUTcpManager, unit>(4243, ())
            let manager = remoteDomain.RemoteActorManager
            manager.PublishActor("appDomainFailingActor")
            manager.StartActor()

            let remoteDomain2 = new RemoteDomainManager<SimpleStateActorUTcpManager<int>, int>(4244, 0)
            let manager2 = remoteDomain2.RemoteActorManager
            manager2.PublishActor("simpleStateActor")
            manager2.StartActor()

            let actorRef = new ActorRef<SimpleStateActor<int>>(ActorUUID.Empty, "appDomainFailingActor", [| new Protocol<SimpleStateActor<int>>(ActorUUID.Empty, "appDomainFailingActor", ClientAddress <| Address.Parse("127.0.0.1:4243")) |])
            let actorRef2 = new ActorRef<SimpleStateActor<int>>(ActorUUID.Empty, "simpleStateActor", [| new Protocol<SimpleStateActor<int>>(ActorUUID.Empty, "simpleStateActor", ClientAddress <| Address.Parse("127.0.0.1:4244")) |])

            printfn "actorRef2 <-- SimpleStateSet 42"
            actorRef2 <-- SimpleStateSet 42
            //this should fail the appdomain
            printfn "actorRef <-- SimpleStateSet 42"
            actorRef <-- SimpleStateSet 42
            printfn "KILLING APPDOMAIN"
            (remoteDomain :> IDisposable).Dispose()

            try
                printfn "ignore (actorRef <!= SimpleStateGet)"
                ignore (actorRef <!= SimpleStateGet)
            with :? CommunicationException ->
                ()

            printfn "let result = actorRef2 <!= SimpleStateGet"
            let result = actorRef2 <!= SimpleStateGet

            result |> should equal 42

//
//        [<Test>]
//        member __.``Connection Pool Exhaustion bug``() =
//            






