namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit
open Nessos.Thespian
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

[<AbstractClass>]
type ``AppDomain Communication``<'T when 'T :> RemoteActorManager and 'T : (new : unit -> 'T)>() = 
    let actorManager = new 'T() :> RemoteActorManager

    abstract PublishActorPrimary : Actor<'U> -> Actor<'U>
    abstract RefPrimary : Actor<'U> -> ActorRef<'U>
    abstract ParallelPostsNum : int
    abstract ParallelAsyncPostsNum : int
    abstract ParallelPostsWithReplyNum : int
    abstract ParallelPostsWithDeserializedNum : int

    member __.ActorManager = actorManager
    
    [<OneTimeTearDown>]
    member __.TestTearDown() = (actorManager :> IDisposable).Dispose()

    [<Test>]
    member self.``Post via ref``() = 
        let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
        actorRef <-- TestAsync 42
        let result = actorRef <!= fun ch -> TestSync(ch, 43)
        result |> should equal 42
    
    [<Test>]
    member self.``Post with reply method``() = 
        let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
        actorRef.Post(TestAsync 42)
        let r = Async.RunSynchronously <| actorRef.PostWithReply(fun ch -> TestSync(ch, 43))
        r |> should equal 42
    
    [<Test>]
    member self.``Post with reply operator``() = 
        let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
        actorRef <-- TestAsync 42
        let r = actorRef <!= fun ch -> TestSync(ch, 43)
        r |> should equal 42
    
    [<Test>]
    [<Timeout(60000)>]
     //make sure the default timeout is less than the test case timeout
     member self.``Post with reply with no timeout (default timeout)``() = 
        Assert.throws<TimeoutException>(fun () ->
            let actorRef = actorManager.CreateActor<TestMessage<unit, unit>>(PrimitiveBehaviors.nill)
            actorRef <!= fun ch -> TestSync(ch, ()))
    
    [<Test>]
    member self.``Post with reply method with timeout (in-time)``() = 
        let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
        actorRef <-- TestAsync 42
        let r = 
            actorRef.PostWithReply
                ((fun ch -> TestSync(ch, Default.ReplyReceiveTimeout / 4)), Default.ReplyReceiveTimeout * 4) 
            |> Async.RunSynchronously
        r |> should equal 42
    
    [<Test>]
    member self.``Post with reply method with timeout``() = 
        Assert.throws<TimeoutException>(fun () ->
            let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
            actorRef.PostWithReply
                ((fun ch -> TestSync(ch, Default.ReplyReceiveTimeout * 4)), Default.ReplyReceiveTimeout / 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply operator with timeout on reply channel (fluid)``() = 
        Assert.throws<TimeoutException>(fun () ->
            let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
            actorRef <!- fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout / 4), Default.ReplyReceiveTimeout * 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply operator with timeout on reply channel (property-set)``() = 
        Assert.throws<TimeoutException>(fun () ->
            let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
            actorRef <!- fun ch -> 
                ch.Timeout <- Default.ReplyReceiveTimeout / 4
                TestSync(ch, Default.ReplyReceiveTimeout * 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply method timeout (fluid) on reply channel overrides method timeout arg``() = 
        Assert.throws<TimeoutException>(fun () ->
            let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
            actorRef <-- TestAsync 42
            actorRef.PostWithReply
                ((fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout / 4), Default.ReplyReceiveTimeout * 8)), 
                 Default.ReplyReceiveTimeout * 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Post with reply method timeout (property set) on reply channel overrides method timeout arg``() = 
        Assert.throws<TimeoutException>(fun () ->
            let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.delayedState)
            actorRef <-- TestAsync 42
            actorRef.PostWithReply((fun ch -> 
                                   ch.Timeout <- Default.ReplyReceiveTimeout / 4
                                   TestSync(ch, Default.ReplyReceiveTimeout * 8)), Default.ReplyReceiveTimeout * 4)
            |> Async.Ignore
            |> Async.RunSynchronously)
    
    [<Test>]
    member self.``Order of posts``() =
        let actorRef = actorManager.CreateActor<TestList<int>>(Behavior.stateful [] Behaviors.list)
        let msgs = 
            [ for i in 1..200 -> ListPrepend i ] @ [ Delay 500 ] @ [ for i in 1..200 -> ListPrepend i ]
        for msg in msgs do
            actorRef <-- msg
        let result = actorRef <!= ListGet
        
        let expected = 
            [ for i in 1..200 -> ListPrepend i ] @ [ for i in 1..200 -> ListPrepend i ]
            |> List.rev
            |> List.map (function 
                   | ListPrepend i -> i
                   | _ -> failwith "Impossibility")
        result |> should equal expected
    
    [<Test>]
    member self.``Parallel posts``() = 
        let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.adder)
        let N = self.ParallelPostsNum
        [ for i in 1..N -> async { actorRef <-- TestAsync i } ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        let expected = 
            [ for i in 1..N -> i ]
            |> List.reduce (+)
        
        let result = actorRef <!= fun ch -> TestSync(ch, 0)
        result |> should equal expected
    
    [<Test>]
    member self.``Parallel async posts``() = 
        let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.adder)
        let N = self.ParallelAsyncPostsNum
        [ for i in 1..N -> actorRef <-!- TestAsync i ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        let expected = 
            [ for i in 1..N -> i ]
            |> List.reduce (+)
        
        let result = actorRef <!= fun ch -> TestSync(ch, 0)
        result |> should equal expected
    
    [<Test>]
    member self.``Parallel posts with reply``() = 
        let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
        let N = self.ParallelPostsWithReplyNum
        [ for i in 1..N -> actorRef <!- fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout * 8), i) ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        let r = actorRef <!= fun ch -> TestSync(ch, 0)
        r |> should be (greaterThanOrEqualTo 1)
        r |> should be (lessThanOrEqualTo N)
    
    [<Test>]
    member self.``Parallel posts with reply with multiple deserialised refs``() = 
        let actorRef = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
        actorRef <-- TestAsync 42
        let N = self.ParallelPostsWithDeserializedNum
        
        let refs = 
            [ for i in 1..N -> actorRef ]
        
        let result = 
            refs
            |> Seq.map (fun ref -> ref <!- fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout * 8), 0))
            |> Async.Parallel
            |> Async.RunSynchronously
            |> Seq.reduce (+)
        
        result |> should equal 42
    
    [<Test>]
    member self.``Parallel posts with reply with collocated and non-collocated refs``() = 
        use actor = 
            Actor.bind <| Behavior.stateful 0 Behaviors.stateNoUpdateOnSync
            |> self.PublishActorPrimary
            |> Actor.start
        
        let actorRef1 = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.stateNoUpdateOnSync)
        let actorRef2 = actorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.stateNoUpdateOnSync)
        self.RefPrimary(actor) <-- TestAsync 36
        actorRef1 <-- TestAsync 2
        actorRef2 <-- TestAsync 40
        let refs = 
            [ self.RefPrimary(actor)
              actorRef1
              actorRef2 ]
        
        let results = 
            refs
            |> Seq.map (fun actorRef -> async { let! r = actorRef 
                                                         <!- fun ch -> 
                                                             TestSync
                                                                 (ch.WithTimeout(Default.ReplyReceiveTimeout * 8), 0)
                                                return r + 2 })
            |> Async.Parallel
            |> Async.RunSynchronously
        
        results |> should equal [| 38; 4; 42 |]

[<AbstractClass>]
type ``AppDomain Tcp Communication``<'T when 'T :> RemoteActorManager and 'T : (new : unit -> 'T)>() = 
    inherit ``AppDomain Communication``<'T>()
    
    [<Test>]
    member self.``Posts with server connection timeout``() = 
        let actorRef = self.ActorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
        actorRef <-- TestAsync 42
        System.Threading.Thread.Sleep(TimeSpan.FromSeconds(10.0))
        let r = actorRef <!= fun ch -> TestSync(ch, 43)
        r |> should equal 42
    
    [<Test>]
    member self.``Parallel async posts with server connection timeouts``() = 
        let actorRef = self.ActorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.adder)
        let N = self.ParallelAsyncPostsNum
        [ for i in 1..N -> 
              async { 
                  if i = N / 4 || i = N / 3 || i = N / 2 then 
                      //sleep more than connection timeout
                      do! Async.Sleep 10000
                  do! actorRef <-!- (TestAsync i)
              } ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        let expected = 
            [ for i in 1..N -> i ]
            |> List.reduce (+)
        
        let result = actorRef <!= fun ch -> TestSync(ch, 0)
        result |> should equal expected
    
    [<Test>]
    member self.``Parallel posts with reply with server connection timeout``() = 
        let actorRef = self.ActorManager.CreateActor<TestMessage<int, int>>(Behavior.stateful 0 Behaviors.state)
        let N = self.ParallelPostsWithReplyNum
        [ for i in 1..N -> 
              async { 
                  if i = N / 4 || i = N / 3 || i = N / 2 then do! Async.Sleep 10000
                  return! actorRef <!- fun ch -> TestSync(ch.WithTimeout(Default.ReplyReceiveTimeout * 2), i)
              } ]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
        let r = actorRef <!= fun ch -> TestSync(ch, 0)
        r |> should be (greaterThanOrEqualTo 1)
        r |> should be (lessThanOrEqualTo N)