namespace Nessos.Thespian.Tests

open NUnit.Framework
open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.PipeProtocol
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<TestFixture>]
type ``Collocated Npp``() = 
    inherit ``Collocated Remote Communication``()

    override __.ParallelPostsNum = 20
    override __.ParallelAsyncPostsNum = 100
    override __.ParallelPostsWithReplyNum = 30
    override __.ParallelPostsWithDeserializedNum = 2
    override __.PublishActorPrimary(actor : Actor<'T>) = actor |> Actor.publish [ Protocols.npp() ]
    override __.RefPrimary(actor : Actor<'T>) = actor.Ref.[NPP]
    override __.ForeignProtocols = 
        [| { //in-memory for foreign protocol
             new ForeignProtocolProxy() with
                 member __.Publish(a) = a
                 member __.Ref(a) = a.Ref
                 member __.ToString() = "in-memory foreign protocol" }
           { //utcp for foreign protocol
             new ForeignProtocolProxy() with
                 member __.Publish(a) = a |> Actor.publish [ Protocols.utcp() ]
                 member __.Ref(a) = a.Ref.[UTCP]
                 member __.ToString() = "utcp foreign protocol" }
           { //btcp for foreign protocol
             new ForeignProtocolProxy() with
                 member __.Publish(a) = a |> Actor.publish [ Protocols.btcp() ]
                 member __.Ref(a) = a.Ref.[BTCP]
                 member __.ToString() = "btcp foreign protocol" } |]

[<TestFixture>]
type ``AppDomain Npp``() = 
    inherit ``AppDomain Communication``<NppActorManagerFactory>()
    override __.ParallelPostsNum = 2
    override __.ParallelAsyncPostsNum = 2
    override __.ParallelPostsWithReplyNum = 2
    override __.ParallelPostsWithDeserializedNum = 2
    override __.GetAppDomainManager(?appDomainName : string) = 
        new AppDomainManager<NppActorManagerFactory>(?appDomainName = appDomainName)
    override __.PublishActorPrimary(actor : Actor<'T>) = actor |> Actor.publish [ Protocols.npp() ]
    override __.RefPrimary(actor : Actor<'T>) = actor.Ref.[NPP]
