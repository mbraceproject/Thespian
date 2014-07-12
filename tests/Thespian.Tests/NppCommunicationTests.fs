namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.PipeProtocol
open Nessos.Thespian.Tests.TestDefinitions.Remote

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
       //utcp for foreign protocol
       { new ForeignProtocolProxy() with
           override __.Publish(a) = a |> Actor.publish [Protocols.utcp()]
           override __.Ref(a) = a.Ref.[UTCP]
           override __.ToString() = "utcp foreign protocol" }
       //btcp for foreign protocol
       { new ForeignProtocolProxy() with
           override __.Publish(a) = a |> Actor.publish [Protocols.btcp()]
           override __.Ref(a) = a.Ref.[BTCP]
           override __.ToString() = "btcp foreign protocol" }
    |]


[<TestFixture>]
type ``AppDomain Npp``() =
  inherit ``AppDomain Communication``<NppActorManagerFactory>()

  override __.GetAppDomainManager(?appDomainName: string) = new AppDomainManager<NppActorManagerFactory>(?appDomainName = appDomainName)
  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.npp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[NPP]
