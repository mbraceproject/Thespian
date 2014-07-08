namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Remote.PipeProtocol

[<TestFixture>]
type ``Collocated Npp``() =
  inherit ``Collocated Remote Communication``()

  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.npp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[NPP]
  override __.ForeignProtocols =
    [|
       //in-memory for foreign protocol
       { new ForeignProtocolProxy() with
           override __.Publish(a) = a
           override __.Ref(a) = a.Ref
           override __.ToString() = "in-memory foreign protocol" }
    |]
