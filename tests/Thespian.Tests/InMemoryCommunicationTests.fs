namespace Nessos.Thespian.Tests

open NUnit.Framework
open Nessos.Thespian
open Nessos.Thespian.Tests.TestDefinitions

[<TestFixture>]
type ``In-memory``() = 
    inherit ``Collocated Communication``()
    override __.ParallelPostsNum = 30
    override __.ParallelAsyncPostsNum = 30
    override __.ParallelPostsWithReplyNum = 100
    override __.PublishActorPrimary a = a
    override __.RefPrimary(a : Actor<'T>) = a.Ref

    [<Test>]
    member self.``Non-publish actor.Ref serialization failure``() = 
        Assert.throws<ThespianSerializationException>(fun () ->
            use actor = Actor.bind PrimitiveBehaviors.nill
            let serializer = Serialization.defaultSerializer
            serializer.Serialize(actor.Ref) |> ignore)

[<TestFixture>]
type ``In-memory (observable)``() = 
    inherit ``In-memory``()
    override __.PrimaryProtocolFactory = new ObservableTestUtils.ProtocolFactory() :> IPrimaryProtocolFactory
