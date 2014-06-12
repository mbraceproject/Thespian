namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Tests.TestDefinitions

[<TestFixture>]
type ``Unidirectional Tcp communication``() =
  inherit CommunicationTests()

  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.utcp()]
