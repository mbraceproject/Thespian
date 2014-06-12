namespace Nessos.Thespian.Tests

open System
open NUnit.Framework

open Nessos.Thespian

[<TestFixture>]
type ``In-memory communication tests``() =
  inherit CommunicationTests()

  override __.PublishActorPrimary a = a
