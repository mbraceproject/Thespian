namespace Nessos.Thespian.Tests

    open System
    open NUnit.Framework

    open Nessos.Thespian
    open Nessos.Thespian.AsyncExtensions
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote
    open Nessos.Thespian.Remote.TcpProtocol
    open Nessos.Thespian.Tests.FsUnit

    [<TestFixture>]
    type ``In Mempory Tests``() =
        [<Test>] 
        member t.``Simple bind``() =
            let simpleActor = simpleActorByPrimitiveBind()

            simpleActor.Name |> should equal String.Empty

        [<Test>]
        member t.``Simple stateless behavior bind``() =
            let refCell = ref 0
            let simpleActor = Actor.bind <| Behavior.stateless (simpleRefCellActorBehavior refCell)

            simpleActor.Name |> should equal String.Empty

        [<Test>]
        member t.``ActorRef after simple bind unpublished``() =
            let simpleActor = simpleActorByPrimitiveBind()

            let refByProperty = simpleActor.Ref
            let refByOperator = !simpleActor

            refByProperty |> should equal refByOperator

            refByOperator.Protocols.Length |> should equal 1

        [<Test>]
        member t.``Simple actor start/stop``() =
            let simpleActor = simpleActorByPrimitiveBind()
            simpleActor.Start()
            simpleActor.Stop()

            simpleActor.Start()
            simpleActor.Stop()

            simpleActor.Start()
            simpleActor.Stop()

            use startedSimpleActor = simpleActor |> Actor.start
            ()

        [<Test>]
        [<ExpectedException(typeof<ActorInactiveException>)>]
        member t.``Post to stopped actor``() =
            let simpleActor = simpleActorByPrimitiveBind()
            
            !simpleActor <-- SimpleOneWay 1

        [<Test>]
        member t.``Post one way operator``() =
            let refCell = ref 0
            use simpleActor = Actor.bind <| Behavior.stateless (simpleRefCellActorBehavior refCell)
                              |> Actor.start

            !simpleActor <-- SimpleOneWay 1

            //do something for a while
            System.Threading.Thread.Sleep(500)

            refCell.Value |> should equal 1

        [<Test>]
        member t.``Post with reply operator``() =
            let refCell = ref 0
            use simpleActor = Actor.bind <| Behavior.stateless (simpleRefCellActorBehavior refCell)
                              |> Actor.start

            let result = !simpleActor <!= fun ch -> SimpleTwoWay(ch, 1)

            result |> should equal 1

        [<Test>]
        member t.``Post as set and post with reply as get``() =
            use simpleStateActor = simpleStateActor() |> Actor.start

            !simpleStateActor <-- SimpleStateSet 1

            let result = !simpleStateActor <!= SimpleStateGet

            result |> should equal 1

        [<Test>]
        member t.``Order of post from specific client is order of processing on actor.``() =
            use simpleStateActor = simpleStateActor() |> Actor.start

            !simpleStateActor <-- SimpleStateSet 1
            !simpleStateActor <-- SimpleStateSet 2
            !simpleStateActor <-- SimpleStateSet 3
            !simpleStateActor <-- SimpleStateSet 4
            !simpleStateActor <-- SimpleStateSet 1
            !simpleStateActor <-- SimpleStateSet 3
            !simpleStateActor <-- SimpleStateSet 42
            !simpleStateActor <-- SimpleStateSet 7777
            !simpleStateActor <-- SimpleStateSet 42

            let result = !simpleStateActor <!= SimpleStateGet

            result |> should equal 42

        [<Test>]
        [<ExpectedException(typeof<System.Runtime.Serialization.SerializationException>)>]
        member t.``Unpublished actorRef serialization``() =
            let serializer = SerializerRegistry.GetDefaultSerializer()

            let simpleActor = simpleStateActor()

            serializer.Serialize(obj(), !simpleActor) |> ignore


        [<Test>]
        member t.``Parallel posts``() =
            use adder = adderActor() |> Actor.start

            [ for i in 1..42 -> async { !adder <-- SimpleStateSet i } ]
            |> Async.Parallel |> Async.Ignore |> Async.RunSynchronously

            let result = !adder <!= SimpleStateGet

            let expectedResult = [for i in 1..42 -> i] |> List.reduce (+)

            result |> should equal expectedResult

        [<Test>]
        member t.``Post with reply operator in async``() =
            use simpleStateActor = simpleStateActor() |> Actor.start

            !simpleStateActor <-- SimpleStateSet 42

            let resultSync = !simpleStateActor <!= SimpleStateGet

            let resultAsync = async { return! !simpleStateActor <!- SimpleStateGet } |> Async.RunSynchronously

            resultSync |> should equal resultAsync

        [<Test>]
        [<ExpectedException(typeof<ActorInactiveException>)>]
        member t.``Post when started, stop and post``() =
            use simpleStateActor = simpleStateActor() |> Actor.start

            !simpleStateActor <-- SimpleStateSet 42

            let result = !simpleStateActor <!= SimpleStateGet

            result |> should equal 42

            simpleStateActor.Stop()

            !simpleStateActor <-- SimpleStateSet 0


        [<Test>]
        [<ExpectedException(typeof<ActorInactiveException>)>]
        member t.``Actor dispose when not started``() =
            let simpleStateActor = simpleStateActor()
            try
                use a = simpleStateActor
                ()
            with :? ActorInactiveException -> failwith "Disposal should not throw."
                 | _ -> reraise()

            !simpleStateActor <-- SimpleStateSet 42

        [<Test>]
        [<ExpectedException(typeof<ActorInactiveException>)>]
        member t.``Actor dispose when started``() =
            let simpleStateActor = simpleStateActor() |> Actor.start
            try
                use a = simpleStateActor
                ()
            with :? ActorInactiveException -> failwith "Disposal should not throw."
                 | _ -> reraise()

            !simpleStateActor <-- SimpleStateSet 42

        [<Test>]
        member t.``Stateful actor start/stop resets state``() =
            let simpleStateActor = simpleStateActor() |> Actor.start

            !simpleStateActor <-- SimpleStateSet 42

            simpleStateActor.Stop()

            try 
                !simpleStateActor <!= SimpleStateGet |> ignore

                failwith "Actor is not stopped."
            with :? ActorInactiveException ->
                use simpleStateActor = simpleStateActor |> Actor.start

                let result = !simpleStateActor <!= SimpleStateGet

                result |> should equal 0

        [<Test>]
        member t.``Post with exception reply``() =
            use exceptionActor = exceptionActor() |> Actor.start

            !exceptionActor <!= GetUnitReply

            try 
                !exceptionActor <!= GetExceptionReply
                failwith "No exception in reply."
            with MessageHandlingException(_, _, _, e) -> e.Message |> should equal "42"

        [<Test>]
        member t.``Actor information logging``() =
            use loggingActor = loggingActor() |> Actor.start

            let awaitObservable = loggingActor.Log |> Async.AwaitObservable

            let result = 
                async {
                    !loggingActor <-- LogInformationString "Hello World!"
                    return! awaitObservable
                } |> Async.RunSynchronously


            match result with
            | Info, LogSource.Actor(name, uuid), (:? string as infoString) ->
                name |> should equal loggingActor.Name
                uuid |> should equal loggingActor.UUId
                infoString |> should equal "Hello World!"
            | _ -> failwith "Invalid log result"

        [<Test>]
        [<ExpectedException(typeof<ActorInactiveException>)>]
        member t.``Actor behavior failure``() =
            let failingActor = failingActor()

            let awaitObservable = failingActor.Log |> Async.AwaitObservable

            use failingActor = failingActor |> Actor.start

            let result = 
                async {
                    !failingActor <-- ()
                    return! awaitObservable
                } |> Async.RunSynchronously

            match result with
            | Error, LogSource.Actor(name, uuid), (:? ActorFailedException as e) ->
                name |> should equal failingActor.Name
                uuid |> should equal failingActor.UUId
                e.InnerException |> shouldMatch (function :? System.InvalidOperationException -> true | _ -> false)
            | _ -> failwith "Invalid actor failure log."

            !failingActor <-- ()

        [<Test>]
        member t.``Actor refs of same behavior different intances are not equal.``() =
            let firstActor = simpleStateActor()
            let secondActor = simpleStateActor()

            firstActor.Ref |> should not (equal secondActor.Ref)

        [<Test>]
        member t.``Different actors by rename``() =
            let firstActor = simpleStateActor()
            let secondActor = firstActor |> Actor.rename "42"

            secondActor.Name |> should equal "42"

            firstActor.Ref |> should not (equal secondActor.Ref)

        [<Test>]
        [<ExpectedException(typeof<System.TimeoutException>)>]
        member t.``Post with reply with timeout``() =
            use notReplyingActor = notReplyingActor() |> Actor.start
            
            !notReplyingActor <!= fun ch -> DoNotReply(ch.WithTimeout(500))

        [<Test>]
        member t.``Post one way``() =
            let refCell = ref 0
            use simpleActor = Actor.bind <| Behavior.stateless (simpleRefCellActorBehavior refCell)
                              |> Actor.start

            simpleActor.Ref.Post(SimpleOneWay 1)

            //do something for a while
            System.Threading.Thread.Sleep(500)

            refCell.Value |> should equal 1

        [<Test>]
        member t.``Post with reply``() =
            use stateActor = simpleStateActor() |> Actor.start

            stateActor.Ref.Post(SimpleStateSet 42)

            let result = stateActor.Ref.PostWithReply(SimpleStateGet) |> Async.RunSynchronously

            result |> should equal 42

        [<Test>]
        member t.``Try post with reply``() =
            use stateActor = simpleStateActor() |> Actor.start

            stateActor.Ref.Post(SimpleStateSet 42)

            let result = stateActor.Ref.TryPostWithReply(SimpleStateGet, 500) |> Async.RunSynchronously
            
            result |> should equal (Some 42)

        [<Test>]
        [<ExpectedException(typeof<MessageHandlingException>)>]
        member t.``Try post with reply exception``() =
            use exceptionActor = exceptionActor() |> Actor.start

            exceptionActor.Ref.TryPostWithReply(GetExceptionReply, 500) |> Async.RunSynchronously |> ignore

        [<Test>]
        member t.``Try post with reply timeout``() =
            use notReplyingActor = notReplyingActor() |> Actor.start
            
            let result = notReplyingActor.Ref.TryPostWithReply(DoNotReply, 500) |> Async.RunSynchronously

            result |> should equal None

        [<Test>]
        [<ExpectedException(typeof<TcpProtocolConfigurationException>)>]
        member t.``Publish UTcp() without registered TcpListener``() =
            use simpleStateActor = simpleStateActor()
                                   |> Actor.publish [Unidirectional.UTcp()]

            ()

        [<Test>]
        [<ExpectedException(typeof<TcpProtocolConfigurationException>)>]
        member t.``Publish UTcp(endPoint) without registered TcpListener``() =
            use simpleStateActor = simpleStateActor()
                                   |> Actor.publish [Unidirectional.UTcp()]

            ()

        [<Test>]
        member t.``Post to receiver``() =
            use receiver = Receiver.create() |> Receiver.start

            let awaitResult = receiver |> Receiver.toObservable |> Async.AwaitObservable

            !receiver <-- 42
            awaitResult |> Async.RunSynchronously |> should equal 42

        [<Test>]
        [<ExpectedException(typeof<ArgumentException>)>]
        member t.``Invalid actor rename``() =
            simpleStateActor() |> Actor.rename "some/name" |> ignore

