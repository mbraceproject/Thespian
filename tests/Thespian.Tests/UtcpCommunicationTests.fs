namespace Nessos.Thespian.Tests

open System.Net
open NUnit.Framework

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<TestFixture>]
type ``Collocated UTcp``() = 
    inherit ``Tcp communication``()
    override __.ParallelPostsNum = 30
    override __.ParallelAsyncPostsNum = 30
    override __.ParallelPostsWithReplyNum = 100
    override __.ParallelPostsWithDeserializedNum = 10
    override __.PublishActorPrimary(actor : Actor<'T>) = actor |> Actor.publish [ Protocols.utcp() ]
    override __.RefPrimary(actor : Actor<'T>) = actor.Ref.[UTCP]
    override __.PublishActorNonExistingListener(actor : Actor<'T>) = 
        actor.Publish [ Protocols.utcp (new IPEndPoint(IPAddress.Loopback, 3939)) ]

    override __.ForeignProtocols = 
        [| { //in-memory for foreign protocol
             new ForeignProtocolProxy() with
                 member __.Publish(a) = a
                 member __.Ref(a) = a.Ref
                 member __.ToString() = "in-memory foreign protocol" }
           { //btcp for foreign protocol
             new ForeignProtocolProxy() with
                 member __.Publish(a) = a |> Actor.publish [ Protocols.btcp() ]
                 member __.Ref(a) = a.Ref.[BTCP]
                 member __.ToString() = "btcp foreign protocol" } |]

[<TestFixture>]
type ``AppDomain UTcp``() = 
    inherit ``AppDomain Tcp Communication``<RemoteUtcpActorManager>()
    override __.ParallelPostsNum = 30
    override __.ParallelAsyncPostsNum = 30
    override __.ParallelPostsWithReplyNum = 100
    override __.ParallelPostsWithDeserializedNum = 10
    override __.PublishActorPrimary(actor : Actor<'T>) = actor |> Actor.publish [ Protocols.utcp() ]
    override __.RefPrimary(actor : Actor<'T>) = actor.Ref.[UTCP]