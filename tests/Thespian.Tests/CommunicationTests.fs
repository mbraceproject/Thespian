namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Tests.TestDefinitions

[<AbstractClass>]
type ``Collocated Communication``() =
  let mutable defaultPrimaryProtocolFactory = Unchecked.defaultof<IPrimaryProtocolFactory>
  
  abstract PrimaryProtocolFactory: IPrimaryProtocolFactory
  default __.PrimaryProtocolFactory = new MailboxPrimaryProtocolFactory() :> IPrimaryProtocolFactory
  abstract PublishActorPrimary: Actor<'T> -> Actor<'T>
  abstract RefPrimary: Actor<'T> -> ActorRef<'T>

  [<TestFixtureSetUp>]
  member self.SetUp() =
    defaultPrimaryProtocolFactory <- Actor.DefaultPrimaryProtocolFactory
    Actor.DefaultPrimaryProtocolFactory <- self.PrimaryProtocolFactory

  [<TestFixtureTearDown>]
  member self.TearDown() =
    Actor.DefaultPrimaryProtocolFactory <- defaultPrimaryProtocolFactory
  
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
  [<ExpectedException(typeof<TimeoutException>)>]
  [<Timeout(60000)>] //make sure the default timeout is less than the test case timeout
  member self.``Post with reply method with no timeout (default timeout)``() =
    use actor = Actor.bind PrimitiveBehaviors.nill |> self.PublishActorPrimary |> Actor.start
    self.RefPrimary(actor).PostWithReply(fun ch -> TestSync(ch, ()))
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  [<Timeout(60000)>] //make sure the default timeout is less than the test case timeout
  member self.``Post with reply operator with no timeout (default timeout)``() =
    use actor = Actor.bind PrimitiveBehaviors.nill |> self.PublishActorPrimary |> Actor.start
    self.RefPrimary(actor) <!= fun ch -> TestSync(ch, ())

  [<Test>]
  member self.``Post with reply method with timeout (in-time)``() =
    use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                |> self.PublishActorPrimary
                |> Actor.start

    self.RefPrimary(actor) <-- TestAsync 42
    
    let r = self.RefPrimary(actor).PostWithReply((fun ch -> TestSync(ch, Default.ReplyReceiveTimeout/4)), Default.ReplyReceiveTimeout/2)
            |> Async.RunSynchronously
    r |> should equal 42

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply method with timeout``() =
    use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                |> self.PublishActorPrimary
                |> Actor.start
    
    self.RefPrimary(actor).PostWithReply((fun ch -> TestSync(ch, Default.ReplyReceiveTimeout)), Default.ReplyReceiveTimeout/4)
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply operator with timeout on reply channel (fluid)``() =
    use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                |> self.PublishActorPrimary
                |> Actor.start
    
    self.RefPrimary(actor) <!- fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout/4), Default.ReplyReceiveTimeout)
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply operator with timeout on reply channel (property set)``() =
    use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                |> self.PublishActorPrimary
                |> Actor.start
    
    self.RefPrimary(actor) <!- fun ch -> ch.Timeout <- Default.ReplyReceiveTimeout/4; TestSync(ch, Default.ReplyReceiveTimeout)
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply method timeout (fluid) on reply channel overrides method timeout arg``() =
    use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                |> self.PublishActorPrimary
                |> Actor.start

    self.RefPrimary(actor) <-- TestAsync 42

    //the actor will stall for Default.ReplyReceiveTimeout,
    //the reply timeout specified by the method arg is Default.ReplyReceiveTimeout * 2
    //enough to get back the reply
    //the timeout is overriden by setting the reply channel timeout to Default.ReplyReceiveTimeout/2
    //thus we expect this to timeout
    self.RefPrimary(actor).PostWithReply((fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout/2), Default.ReplyReceiveTimeout)), Default.ReplyReceiveTimeout * 2)
    |> Async.Ignore
    |> Async.RunSynchronously

  [<Test>]
  [<ExpectedException(typeof<TimeoutException>)>]
  member self.``Post with reply method timeout (property set) on reply channel overrides method timeout arg``() =
    use actor = Actor.bind <| Behavior.stateful 0 Behaviors.delayedState
                |> self.PublishActorPrimary
                |> Actor.start

    self.RefPrimary(actor) <-- TestAsync 42

    //the actor will stall for Default.ReplyReceiveTimeout,
    //the reply timeout specified by the method arg is Default.ReplyReceiveTimeout * 2
    //enough to get back the reply
    //the timeout is overriden by setting the reply channel timeout to Default.ReplyReceiveTimeout/2
    //thus we expect this to timeout
    self.RefPrimary(actor).PostWithReply((fun ch -> ch.Timeout <- Default.ReplyReceiveTimeout/2; TestSync(ch, Default.ReplyReceiveTimeout)), Default.ReplyReceiveTimeout * 2)
    |> Async.Ignore
    |> Async.RunSynchronously


[<AbstractClass>]
type ``Collocated Remote Communication``() =
  inherit ``Collocated Communication``()

  [<Test>]
  member self.``Post to collocated actor through a non-collocated ref``() =
    use actor = Actor.bind PrimitiveBehaviors.nill |> self.PublishActorPrimary |> Actor.start

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
  [<ExpectedException(typeof<UnknownRecipientException>)>]
  member self.``Post to published stopped actor``() =
    use actor = Actor.bind PrimitiveBehaviors.nill |> self.PublishActorPrimary
    self.RefPrimary(actor) <-- TestAsync()

  [<Test>]
  [<ExpectedException(typeof<UnknownRecipientException>)>]
  member self.``Post when started, stop and post``() =
    use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                |> self.PublishActorPrimary
                |> Actor.start

    self.RefPrimary(actor) <-- TestAsync 42
    let r = self.RefPrimary(actor) <!= fun ch -> TestSync(ch, 43)
    r |> should equal 42

    actor.Stop()
    self.RefPrimary(actor) <-- TestAsync 0

open Nessos.Thespian.Remote

[<AbstractClass>]
type ``Tcp communication``() =
  inherit ``Collocated Remote Communication``()

  abstract PublishActorNonExistingListener: Actor<'T> -> Actor<'T>

  [<Test>]
  [<ExpectedException(typeof<TcpProtocolConfigurationException>)>]
  member self.``Publish protocol on non-existing listener``() =
    use actor = Actor.bind PrimitiveBehaviors.nill |> self.PublishActorNonExistingListener
    ()
