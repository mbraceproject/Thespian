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

  // [<TearDown>]
  // member __.TestTearDown() =
  //   let memoryUsage = GC.GetTotalMemory(true)
  //   printfn "Total Memory = %d bytes" memoryUsage

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

    actorRef.PostWithReply((fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout/4), Default.ReplyReceiveTimeout*8)), Default.ReplyReceiveTimeout*4)
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

    actorRef.PostWithReply((fun ch -> ch.Timeout <- Default.ReplyReceiveTimeout/4; TestSync(ch, Default.ReplyReceiveTimeout*8)), Default.ReplyReceiveTimeout*4)
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

  [<Test>]
  member self.``Posts with server connection timeout``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <-- TestAsync 42
    System.Threading.Thread.Sleep(TimeSpan.FromSeconds(10.0))
    let r = actorRef <!= fun ch -> TestSync(ch, 43)
    r |> should equal 42

  [<Test>]
  member self.``Order of posts``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestList<int>>(Behavior.stateful [] Behaviors.list)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    let msgs =
      [ for i in 1..200 -> ListPrepend i]
      @
      [ Delay 500]
      @
      [ for i in 1..200 -> ListPrepend i]

    for msg in msgs do actorRef <-- msg

    let result = actorRef <!= ListGet

    let expected =
      [ for i in 1..200 -> ListPrepend i]
      @
      [ for i in 1..200 -> ListPrepend i]
      |> List.rev
      |> List.map (function ListPrepend i -> i | _ -> failwith "Impossibility")

    result |> should equal expected

  [<Test>]
  member self.``Parallel posts``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.adder)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    [ for i in 1..30 -> async { actorRef <-- TestAsync i } ]
    |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

    let expected = [ for i in 1..30 -> i ] |> List.reduce (+)
    let result = actorRef <!= fun ch -> TestSync(ch, 0)
    result |> should equal expected

  [<Test>]
  member self.``Parallel async posts``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.adder)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    [ for i in 1..30 -> actorRef <-!- TestAsync i ]
    |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

    let expected = [ for i in 1..30 -> i ] |> List.reduce (+)
    let result = actorRef <!= fun ch -> TestSync(ch, 0)
    result |> should equal expected

  [<Test>]
  member self.``Parallel async posts with server connection timeouts``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.adder)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    [ for i in 1..100 -> async {
      if i = 21 || i = 42 || i = 84 then 
        //sleep more than connection timeout
        do! Async.Sleep 10000
      do! actorRef <-!- (TestAsync i) 
    }] |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

    let expected = [for i in 1..100 -> i] |> List.reduce (+)
    let result = actorRef <!= fun ch -> TestSync(ch, 0)
    result |> should equal expected

  [<Test>]
  member self.``Parallel posts with reply``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    [ for i in 1..100 -> actorRef <!- fun ch -> TestSync(ch, i) ]
    |> Async.Parallel
    |> Async.Ignore
    |> Async.RunSynchronously

    let r = actorRef <!= fun ch -> TestSync(ch, 0)
    r |> should be (greaterThanOrEqualTo 1)
    r |> should be (lessThanOrEqualTo 100)

  [<Test>]
  member self.``Parallel posts with reply with server connection timeout``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    [ for i in 1..100 -> async {
        if i = 21 || i = 42 || i = 84 then
          do! Async.Sleep 10000
        return! actorRef <!- fun ch -> TestSync(ch, i)
    }] |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

    let r = actorRef <!= fun ch -> TestSync(ch, 0)
    r |> should be (greaterThanOrEqualTo 1)
    r |> should be (lessThanOrEqualTo 100)
