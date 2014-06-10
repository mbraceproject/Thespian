module Nessos.Thespian.Tests.TestDefinitions

open System
open Nessos.Thespian

type TestMessage<'T, 'R> =
  | TestAsync of 'T
  | TestSync of IReplyChannel<'R> * 'T

type TestMessage<'T> = TestMessage<'T, unit>


module PrimitiveBehaviors =
  let nill (self: Actor<TestMessage<unit>>) = async.Zero()
