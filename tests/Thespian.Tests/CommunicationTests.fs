namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Tests.TestDefinitions

[<AbstractClass>]
type ``Collocated Communication``() =

  abstract PublishActorPrimary: Actor<'T> -> Actor<'T>
  abstract RefPrimary: Actor<'T> -> ActorRef<'T>
  
  [<Test>]
  member self.``Post``() =
    let cell = ref 0
    use actor = Actor.bind <| Behavior.stateless (Behaviors.refCell cell)
                |> self.PublishActorPrimary
                |> Actor.start

    self.RefPrimary(actor).Post(TestAsync 42)
    //do something for a while
    System.Threading.Thread.Sleep(500)
    cell.Value |> should equal 42

  [<Test>]
  member self.``Post with reply``() =
    use actor = Actor.bind <| Behavior.stateful 0 Behaviors.state
                |> self.PublishActorPrimary
                |> Actor.start

    self.RefPrimary(actor).Post(TestAsync 42)
    let r = Async.RunSynchronously <| self.RefPrimary(actor).PostWithReply(fun ch -> TestSync(ch, 43))
    r |> should equal 42


