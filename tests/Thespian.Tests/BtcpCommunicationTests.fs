namespace Nessos.Thespian.Tests

open System
open System.Net
open NUnit.Framework
open FsUnit

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.PipeProtocol
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<TestFixture>]
type ``Collocated BTcp``() =
  inherit ``Tcp communication``()

  override __.PublishActorPrimary(actor: Actor<'T>) = actor |> Actor.publish [Protocols.btcp()]
  override __.RefPrimary(actor: Actor<'T>) = actor.Ref.[BTCP]
  override __.PublishActorNonExistingListener(actor: Actor<'T>) =
    actor.Publish [Protocols.utcp(new IPEndPoint(IPAddress.Loopback, 3939))]
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
       //npp for foreign protocol
       { new ForeignProtocolProxy() with
           override __.Publish(a) = a |> Actor.publish [Protocols.npp()]
           override __.Ref(a) = a.Ref.[NPP]
           override __.ToString() = "npp foreign protocol" }
    |]

[<TestFixture>]
type ``AppDomain BTcp``() =
  inherit ``AppDomain Tcp Communication``<BtcpActorManagerFactory>()

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
