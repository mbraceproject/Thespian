namespace Nessos.Thespian.Tests

open System
open System.Net
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Remote.PipeProtocol
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<TestFixture>]
type ``Collocated UTcp``() =
  inherit ``Tcp communication``()

  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.utcp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[UTCP]
  override __.PublishActorNonExistingListener(actor: Actor<'T>) =
    actor.Publish [Protocols.utcp(new IPEndPoint(IPAddress.Loopback, 3939))]
  override __.ForeignProtocols =
    [|
       //in-memory for foreign protocol
       { new ForeignProtocolProxy() with
           override __.Publish(a) = a
           override __.Ref(a) = a.Ref
           override __.ToString() = "in-memory foreign protocol" }
       //btcp for foreign protocol
       { new ForeignProtocolProxy() with
           override __.Publish(a) = a |> Actor.publish [Protocols.btcp()]
           override __.Ref(a) = a.Ref.[BTCP]
           override __.ToString() = "btcp foreign protocol" }
       //npp for foreign protocol
       { new ForeignProtocolProxy() with
           override __.Publish(a) = a |> Actor.publish [Protocols.npp()]
           override __.Ref(a) = a.Ref.[NPP]
           override __.ToString() = "npp foreign protocol" }
    |]


[<TestFixture>]
type ``AppDomain UTcp``() =
  inherit ``AppDomain Tcp Communication``<UtcpActorManagerFactory>()

  override __.GetAppDomainManager(?appDomainName: string) = new AppDomainManager<UtcpActorManagerFactory>(?appDomainName = appDomainName)
  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.utcp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[UTCP]
