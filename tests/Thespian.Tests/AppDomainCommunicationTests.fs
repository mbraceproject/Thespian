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

  member __.WrapBehavior(behavior: Actor<'T> -> Async<unit>) =
    let serializer = Serialization.defaultSerializer
    serializer.Serialize(behavior)

  [<Test>]
  member self.``Post via ref``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(BehaviorValue.Create<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state))
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <-- TestAsync 42
    let result = actorRef <!= fun ch -> TestSync(ch, 43)
    result |> should equal 42

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  [<Timeout(60000)>] //make sure the default timeout is less than the test case timeout
  member self.``Post with reply with no timeout (default timeout)``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<unit, unit>>(BehaviorValue.Create PrimitiveBehaviors.nill)
    let actorRef = actorManager.Publish()
    actorManager.Start()
    
    actorRef <!= fun ch -> TestSync(ch, ())


  [<Test>]
  [<ExpectedException(typeof<UnknownRecipientException>)>]
  member self.``Post when stopped``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<unit, unit>>(BehaviorValue.Create PrimitiveBehaviors.nill)
    let actorRef = actorManager.Publish()

    actorRef <-- TestAsync()

  [<Test>]
  [<ExpectedException(typeof<UnknownRecipientException>)>]
  member self.``Post when started, stop and post``() =
    use appDomainManager = self.GetAppDomainManager()
    let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(BehaviorValue.Create <| Behavior.stateful 0 Behaviors.state)
    let actorRef = actorManager.Publish()
    actorManager.Start()

    actorRef <-- TestAsync 42
    let r = actorRef <!= fun ch -> TestSync(ch, 43)
    r |> should equal 42

    actorManager.Stop()
    actorRef <-- TestAsync 0

