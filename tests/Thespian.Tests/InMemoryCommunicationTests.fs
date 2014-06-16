namespace Nessos.Thespian.Tests

open System
open System.Runtime.Serialization
open NUnit.Framework

open Nessos.Thespian
open Nessos.Thespian.Tests.TestDefinitions

[<TestFixture>]
type ``In-memory``() =
  inherit ``Collocated Communication``()

  override __.PublishActorPrimary a = a
  override __.RefPrimary(a: Actor<'T>) = a.Ref

  [<Test>]
  [<ExpectedException(typeof<SerializationException>)>]
  member self.``Non-publish actor.Ref serialization failure``() =
    use actor = Actor.bind PrimitiveBehaviors.nill

    let serializer = Serialization.defaultSerializer
    serializer.Serialize(actor.Ref) |> ignore

[<TestFixture>]
type ``In-memory (observable)``() =
  inherit ``In-memory``()

  override __.PrimaryProtocolFactory = new ObservableTestUtils.ProtocolFactory() :> IPrimaryProtocolFactory

