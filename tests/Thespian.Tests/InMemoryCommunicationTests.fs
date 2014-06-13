namespace Nessos.Thespian.Tests

open System
open NUnit.Framework

open Nessos.Thespian

[<TestFixture>]
type ``In-memory``() =
  inherit ``Collocated Communication``()

  override __.PublishActorPrimary a = a
  override __.RefPrimary(a: Actor<'T>) = a.Ref
