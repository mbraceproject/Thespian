namespace Nessos.Thespian.Tests

open System
open System.Net
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<TestFixture>]
type ``Collocated BTcp``() =
  inherit ``Tcp communication``()

  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.btcp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[BTCP]
  override __.PublishActorNonExistingListener(actor: Actor<'T>) =
    actor.Publish [Protocols.utcp(new IPEndPoint(IPAddress.Loopback, 3939))]


[<TestFixture>]
type ``AppDomain BTcp``() =
  inherit ``AppDomain Communication``<BtcpActorManagerFactory>()

  // [<TestFixtureSetUp>]
  // member __.Init() =
  //   let rec printConnectionPoolCounters() =
  //     async {
  //       printfn "%A" <| TcpProtocol.ConnectionPool.TcpConnectionPool.GetCounters()
  //       do! Async.Sleep 1000
  //       return! printConnectionPoolCounters()
  //     }

  //   printConnectionPoolCounters()
  //   |> Async.Ignore
  //   |> Async.Start

  override __.GetAppDomainManager(?appDomainName: string) = new AppDomainManager<BtcpActorManagerFactory>(?appDomainName = appDomainName)
  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.btcp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[BTCP]
