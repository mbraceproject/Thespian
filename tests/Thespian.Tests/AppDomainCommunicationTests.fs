namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<AbstractClass>]
type ``AppDomain Communication``<'T when 'T :> ActorManagerFactory>() =
  abstract GetAppDomainManager: ?appDomainName: string -> AppDomainManager<'T>

  [<TearDown>]
  member __.TestTearDown() =
    let memoryUsage = GC.GetTotalMemory(true)
    printfn "Total Memory = %A" memoryUsage

  [<Test>]
  member self.``Post via ref``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <-- TestAsync 42
    let result = actorRef <!= fun ch -> TestSync(ch, 43)
    result |> should equal 42

  [<Test>]
  member self.``Post with reply method``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef.Post(TestAsync 42)
    let r = Async.RunSynchronously <| actorRef.PostWithReply(fun ch -> TestSync(ch, 43))
    r |> should equal 42

  [<Test>]
  member self.``Post with reply operator``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <-- TestAsync 42
    let r = actorRef <!= fun ch -> TestSync(ch, 43)
    r |> should equal 42

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  [<Timeout(60000)>] //make sure the default timeout is less than the test case timeout
  member self.``Post with reply with no timeout (default timeout)``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<unit, unit>>(PrimitiveBehaviors.nill)
    let actorRef = actorManager.Publish()
    actorManager.Start()
    
    actorRef <!= fun ch -> TestSync(ch, ())

  [<Test>]
  member self.``Post with reply method with timeout (in-time)``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <-- TestAsync 42
    let r = actorRef.PostWithReply((fun ch -> TestSync(ch, Default.ReplyReceiveTimeout/4)), Default.ReplyReceiveTimeout*4)
            |> Async.RunSynchronously
    r |> should equal 42

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply method with timeout``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef.PostWithReply((fun ch -> TestSync(ch, Default.ReplyReceiveTimeout*4)), Default.ReplyReceiveTimeout/4)
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply operator with timeout on reply channel (fluid)``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <!- fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout/4), Default.ReplyReceiveTimeout*4)
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply operator with timeout on reply channel (property-set)``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <!- fun ch -> ch.Timeout <- Default.ReplyReceiveTimeout/4; TestSync(ch, Default.ReplyReceiveTimeout*4)
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply method timeout (fluid) on reply channel overrides method timeout arg``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <-- TestAsync 42

    actorRef.PostWithReply((fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout/4), Default.ReplyReceiveTimeout)), Default.ReplyReceiveTimeout*4)
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply method timeout (property set) on reply channel overrides method timeout arg``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <-- TestAsync 42

    actorRef.PostWithReply((fun ch -> ch.Timeout <- Default.ReplyReceiveTimeout/4; TestSync(ch, Default.ReplyReceiveTimeout)), Default.ReplyReceiveTimeout*4)
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<UnknownRecipientException>)>]
  member self.``Post when stopped``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<unit, unit>>(PrimitiveBehaviors.nill)
    let actorRef = actorManager.Publish()

    actorRef <-- TestAsync()

  [<Test>]
  [<ExpectedException(typeof<UnknownRecipientException>)>]
  member self.``Post when started, stop and post``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <-- TestAsync 42
    let r = actorRef <!= fun ch -> TestSync(ch, 43)
    r |> should equal 42

    actorManager.Stop()
    actorRef <-- TestAsync 0

