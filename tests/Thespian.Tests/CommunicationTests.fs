namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Tests.TestDefinitions

[<AbstractClass>]
type CommunicationTests() =

  abstract PublishActorPrimary: Actor<'T> -> Actor<'T>
  
  [<Test>]
  member self.``Post with method on ref``() =
    let cell = ref 0
    use actor = Actor.bind <| Behavior.stateless (Behaviors.refCell cell)
                |> self.PublishActorPrimary
                |> Actor.start

    actor.Ref.Post(TestAsync 42)
    //do something for a while
    System.Threading.Thread.Sleep(500)
    cell.Value |> should equal 42

  [<Test>]
  member self.``Post with operator``() =
    let cell = ref 0
    use actor = Actor.bind <| Behavior.stateless (Behaviors.refCell cell)
                |> self.PublishActorPrimary
                |> Actor.start

    !actor <-- TestAsync 42
    //do something for a while
    System.Threading.Thread.Sleep(500)
    cell.Value |> should equal 42
