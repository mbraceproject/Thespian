namespace Nessos.Thespian.Tests

open System
open System.Net
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<TestFixture>]
type ``Collocated UTcp``() =
  inherit ``Tcp communication``()

  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.utcp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[UTCP]
  override __.PublishActorNonExistingListener(actor: Actor<'T>) =
    actor.Publish [Protocols.utcp(new IPEndPoint(IPAddress.Loopback, 3939))]


[<TestFixture>]
type ``AppDomain UTcp``() =
  inherit ``AppDomain Communication``<UtcpActorManagerFactory>()

  override __.GetAppDomainManager(?appDomainName: string) = new AppDomainManager<UtcpActorManagerFactory>(?appDomainName = appDomainName)
