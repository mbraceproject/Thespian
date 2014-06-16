namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<TestFixture>]
type ``Collocated BTcp``() =
  inherit ``Collocated Remote Communication``()

  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.btcp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[BTCP]


[<TestFixture>]
type ``AppDomain BTcp``() =
  inherit ``AppDomain Communication``<BtcpActorManagerFactory>()

  override __.GetAppDomainManager(?appDomainName: string) = new AppDomainManager<BtcpActorManagerFactory>(?appDomainName = appDomainName)

