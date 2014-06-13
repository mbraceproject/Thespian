namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<TestFixture>]
type ``Collocated UTcp``() =
  inherit ``Collocated Communication``()

  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.utcp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[UTCP]


[<TestFixture>]
type ``AppDomain UTcp``() =
  inherit ``AppDomain Communication``<UtcpActorManagerFactory>()

  override __.GetAppDomainManager(?appDomainName: string) = new AppDomainManager<UtcpActorManagerFactory>(?appDomainName = appDomainName)
