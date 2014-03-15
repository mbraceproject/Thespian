namespace Thespian.Tests

    open System
    open Thespian
    open Thespian.Remote
    open Thespian.Remote.ConnectionPool

    [<AutoOpen>]
    module TestActors =
        
        type SimpleMessageType<'T, 'R> =
            | SimpleOneWay of 'T
            | SimpleTwoWay of IReplyChannel<'R> * 'T

        type SimpleMessageType<'T> = SimpleMessageType<'T, unit>

        let simpleRefCellActorBehavior (refCell: 'T ref) (msg: SimpleMessageType<'T, 'T>) = async {
            match msg with
            | SimpleOneWay v -> refCell := v
            | SimpleTwoWay(R(reply), v) -> reply <| Value v
        }

        let rec simpleRefCellActorPrimitiveBehavior (refCell: 'T ref) (actor: Actor<SimpleMessageType<'T, 'T>>) = async {
            let! msg = actor.Receive()

            do! simpleRefCellActorBehavior refCell msg

            return! simpleRefCellActorPrimitiveBehavior refCell actor
        }

        let simpleActorByPrimitiveBind() = 
            let refCell = ref 0
            Actor.bind (simpleRefCellActorPrimitiveBehavior refCell)

        type SimpleStateActor<'T> =
            | SimpleStateSet of 'T
            | SimpleStateGet of IReplyChannel<'T>

        type ListActor<'T> =
            | ListPrepend of 'T
            | GetList of IReplyChannel<'T list>
            | Delay of int

        let simpleStatefulBehavior (state: 'T) (msg: SimpleStateActor<'T>) = async {
            match msg with
            | SimpleStateSet v -> sprintfn "SimpleStateSet"; return v
            | SimpleStateGet(R(reply)) -> reply <| Value state; sprintfn "SimpleStateGet %A" state; return state
        }

        let orderOfPostsBehavior (state: 'T list) (msg: ListActor<'T>) = async {
            match msg with
            | Delay t -> 
                do! Async.Sleep t
                return state
            | ListPrepend v -> return v::state
            | GetList(R reply) -> reply <| Value state; return state
        }

        let orderOfPostsActor (init: 'T list) =
            Actor.bind <| Behavior.stateful init orderOfPostsBehavior

        let simpleStateActor() =
            Actor.bind <| Behavior.stateful 0 simpleStatefulBehavior

        let simpleStateActorGeneric (init: 'T) =
            Actor.bind <| Behavior.stateful init simpleStatefulBehavior

        type ConfirmedStateActor<'T> =
            | ConfirmedStateSet of IReplyChannel<unit> * 'T
            | ConfirmedStateGet of IReplyChannel<'T>

        let confirmedStateActor() =
            Actor.bind <| Behavior.stateful 0 (fun state msg -> async {
                match msg with
                | ConfirmedStateSet(R(reply), v) -> reply nothing; sprintfn "ConfirmedStateSet %A" v; return v
                | ConfirmedStateGet(R(reply)) -> reply <| Value state; sprintfn "ConfirmedStateGet %A" state; return state
            })

        let adderActor() =
            Actor.bind <| Behavior.stateful 0 (fun acc msg -> async {
                match msg with
                | SimpleStateSet i -> return acc + i
                | SimpleStateGet(R(reply)) -> reply <| Value acc; return acc
            })

        type UnitOrExceptionReply = GetUnitReply of IReplyChannel<unit> | GetExceptionReply of IReplyChannel<unit>

        let exceptionActor() =
            Actor.bind <| Behavior.stateless (fun msg -> async {
                match msg with
                | GetUnitReply(R(reply)) -> reply nothing
                | GetExceptionReply(R(reply)) -> reply <| Exception (new Exception("42"))
            })

        type FailMessage = MessageHandlingFail

        let failingActor() =
            Actor.bind <| Behavior.stateless (fun _ -> async { return invalidOp "The actor throws an unhandled exception." }) : Actor<unit>

        let failingActor2() =
            Actor.bind (fun actor ->
                let rec behavior() = async {
                    let! msg = actor.Receive()
                    match msg with
                    | 42 -> ()
                    | _ -> invalidOp "The actor throws an unhandled exception."

                    return! behavior()
                }

                behavior()
            )

        type LoggingMessage = LogInformationString of string | PingPong of IReplyChannel<unit>

        let loggingActor() =
            Actor.bind (fun actor ->
                let rec behavior () = async {
                    let! msg = actor.Receive()
                    match msg with 
                    | LogInformationString m -> actor.LogInfo(m)
                    | PingPong(R(reply)) -> reply nothing
                    return! behavior ()
                }

                behavior()
            )

        type NotReplyingMessage = DoNotReply of IReplyChannel<unit>

        let notReplyingActor() =
            Actor.sink() : Actor<NotReplyingMessage>

        let intermediateActor (target: ActorRef<'T>) = 
            Actor.bind <| Behavior.stateless (fun msg -> async {
                let message = msg
                target <-- msg
            })

        type MultipleRepliesStateActor<'T> =
            | MupltipleRepliesStateSet of 'T
            | MupltipleRepliesStateGet of IReplyChannel<unit> * IReplyChannel<'T>

        let multipleRepliesStateActor (init: 'T) =
            Actor.bind <| Behavior.stateful init (fun v msg -> async {
                match msg with
                | MupltipleRepliesStateSet v -> return v
                | MupltipleRepliesStateGet(R(reply1), R(reply2)) -> reply1 nothing; reply2 <| Value v; return v
            })

        let mupltipleRepliesMap (actorRef: ActorRef<MultipleRepliesStateActor<'T>>) =
            Actor.bind <| Behavior.stateless (fun msg -> async {
                match msg with
                | SimpleStateSet v -> actorRef <-- MupltipleRepliesStateSet v
                | SimpleStateGet rc -> do! actorRef <!- fun ch -> MupltipleRepliesStateGet(ch, rc)
            })


        let appdomainFailActor() =
            Actor.bind <| Behavior.stateful 0 (fun acc msg -> async {
                match msg with
                | SimpleStateSet i -> return i
                | SimpleStateGet(R(reply)) -> reply <| Value acc; return acc
            })

        let appdomainFailActor2() =
            Actor.bind <| Behavior.stateful 0 (fun acc msg -> async {
                match msg with
                | SimpleStateSet i -> return i
                | SimpleStateGet _-> AppDomain.Unload(AppDomain.CurrentDomain); return acc
            })

    module RemoteTesting =
        open System
        open System.Net
        open System.Runtime.Serialization
        open Thespian.Remote.TcpProtocol

        [<Serializable>]
        type NonDeserializable(fail: bool) =
            new (info: SerializationInfo, context: StreamingContext) =
                let fail = info.GetBoolean("fail")
                if fail then invalidOp "Cannot deserialize this instance."
                NonDeserializable(fail)

            interface ISerializable with
                member ch.GetObjectData(info: SerializationInfo, context: StreamingContext) =
                    info.AddValue("fail", fail)


        type RemoteActorManager<'T> =
            abstract Init: System.IO.TextWriter * int * 'T -> unit
            abstract Fini: unit -> unit

        type SimpleStateActorUTcpManager<'T>() =
            inherit MarshalByRefObject()

            let mutable actor = Actor.sink()

            member f.Init(textWriter: System.IO.TextWriter, port: int, initState: 'T) =
                Console.SetOut(textWriter)

                printfn "INITINT"
                TcpListenerPool.DefaultHostname <- "localhost"
                TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), concurrentAccepts = 10)
                TcpConnectionPool.Init()
                actor <- simpleStateActorGeneric initState
                printfn "%A" actor.Ref
            member f.Fini() =
                actor.Stop()
                TcpConnectionPool.Fini()
                TcpListenerPool.Clear()

            member f.PublishActor() =
                actor <- actor |> Actor.publish [Unidirectional.UTcp()]
            member f.PublishActor(name: string) =
                actor <- actor
                         |> Actor.rename name
                         |> Actor.publish [Unidirectional.UTcp()]

            member f.StartActor() =
                actor.Start()
            member f.StopActor() =
                actor.Stop()

            member f.ActorRef = actor.Ref

            interface RemoteActorManager<'T> with
                member f.Init(textWriter: System.IO.TextWriter, port: int, init: 'T) = f.Init(textWriter, port, init)
                member f.Fini() = f.Fini()

        type SimpleStateActorBTcpManager<'T>() =
            inherit MarshalByRefObject()

            let mutable actor = Actor.sink()

            member f.Init(port: int, initState: 'T) =
                TcpListenerPool.DefaultHostname <- "localhost"
                TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), concurrentAccepts = 10)
                TcpConnectionPool.Init()
                actor <- simpleStateActorGeneric initState
            member f.Fini() =
                actor.Stop()
                TcpConnectionPool.Fini()
                TcpListenerPool.Clear()

            member f.PublishActor() =
                actor <- actor |> Actor.publish [Bidirectional.BTcp()]
            member f.PublishActor(name: string) =
                printfn "Actor: %A" actor
                actor <- actor
                         |> Actor.rename name
                         |> Actor.publish [Bidirectional.BTcp()]

            member f.StartActor() =
                actor.Start()
            member f.StopActor() =
                actor.Stop()

            member f.ActorRef = actor.Ref

            interface RemoteActorManager<'T> with
                member f.Init(_, port: int, init: 'T) = f.Init(port, init)
                member f.Fini() = f.Fini()

        type IntermediateActorBTcpManager<'T>() =
            inherit MarshalByRefObject()

            let mutable actor = Actor.sink()

            member f.Init(target: ActorRef<'T>) =
                actor <- intermediateActor target

            member f.Init(port: int) =
                TcpListenerPool.DefaultHostname <- "localhost"
                TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), concurrentAccepts = 10)
                TcpConnectionPool.Init()

            member f.Fini() =
                actor.Stop()
                TcpConnectionPool.Fini()
                TcpListenerPool.Clear()

            member f.PublishActor() =
                actor <- actor |> Actor.publish [Bidirectional.BTcp()]
            member f.PublishActor(name: string) =
                actor <- actor
                         |> Actor.rename name
                         |> Actor.publish [Bidirectional.BTcp()]

            member f.StartActor() =
                actor.Start()
            member f.StopActor() =
                actor.Stop()

            member f.ActorRef = actor.Ref

            interface RemoteActorManager<unit> with
                member f.Init(_, port: int, init: unit) = f.Init(port)
                member f.Fini() = f.Fini()

        type FailingActorUTcpManager() =
            inherit MarshalByRefObject()

            let mutable actor = Actor.sink()

            member f.Init(port: int) =
                TcpListenerPool.DefaultHostname <- "localhost"
                TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), concurrentAccepts = 10)
                TcpConnectionPool.Init()
                actor <- failingActor2()
                
            member f.Fini() =
                actor.Stop()
                TcpConnectionPool.Fini()
                TcpListenerPool.Clear()

            member f.PublishActor() =
                actor <- actor |> Actor.publish [Unidirectional.UTcp()]
            member f.PublishActor(name: string) =
                actor <- actor
                         |> Actor.rename name
                         |> Actor.publish [Unidirectional.UTcp()]

            member f.StartActor() =
                actor.Start()
            member f.StopActor() =
                actor.Stop()

            member f.ActorRef = actor.Ref

            interface RemoteActorManager<unit> with
                member f.Init(_, port: int, init: unit) = f.Init(port)
                member f.Fini() = f.Fini()

        type AppDomainFailingActorUTcpManager() =
            inherit MarshalByRefObject()

            let mutable actor = Actor.sink()

            member f.Init(port: int) =
                TcpListenerPool.DefaultHostname <- "localhost"
                TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), concurrentAccepts = 10)
                TcpConnectionPool.Init(0, 1, 500)
                actor <- appdomainFailActor()
                
            member f.Fini() =
                actor.Stop()
                TcpConnectionPool.Fini()
                TcpListenerPool.Clear()

            member f.PublishActor() =
                actor <- actor |> Actor.publish [Unidirectional.UTcp()]
            member f.PublishActor(name: string) =
                actor <- actor
                         |> Actor.rename name
                         |> Actor.publish [Unidirectional.UTcp()]

            member f.StartActor() =
                actor.Start()
            member f.StopActor() =
                actor.Stop()

            member f.ActorRef = actor.Ref

            interface RemoteActorManager<unit> with
                member f.Init(_, port: int, init: unit) = f.Init(port)
                member f.Fini() = f.Fini()

        type AppDomainFailingActorUTcpManager2() =
            inherit MarshalByRefObject()

            let mutable actor = Actor.sink()

            member f.Init(port: int) =
                TcpListenerPool.DefaultHostname <- "localhost"
                TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), concurrentAccepts = 10)
                TcpConnectionPool.Init()
                actor <- appdomainFailActor2()
                
            member f.Fini() =
                actor.Stop()
                TcpConnectionPool.Fini()
                TcpListenerPool.Clear()

            member f.PublishActor() =
                actor <- actor |> Actor.publish [Unidirectional.UTcp()]
            member f.PublishActor(name: string) =
                actor <- actor
                         |> Actor.rename name
                         |> Actor.publish [Unidirectional.UTcp()]

            member f.StartActor() =
                actor.Start()
            member f.StopActor() =
                actor.Stop()

            member f.ActorRef = actor.Ref

            interface RemoteActorManager<unit> with
                member f.Init(_, port: int, init: unit) = f.Init(port)
                member f.Fini() = f.Fini()

        type FailingActorBTcpManager() =
            inherit MarshalByRefObject()

            let mutable actor = Actor.sink()

            member f.Init(port: int) =
                TcpListenerPool.DefaultHostname <- "localhost"
                TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), concurrentAccepts = 10)
                TcpConnectionPool.Init()
                actor <- failingActor2()
                
            member f.Fini() =
                actor.Stop()
                TcpConnectionPool.Fini()
                TcpListenerPool.Clear()

            member f.PublishActor() =
                actor <- actor |> Actor.publish [Bidirectional.BTcp()]
            member f.PublishActor(name: string) =
                actor <- actor
                         |> Actor.rename name
                         |> Actor.publish [Bidirectional.BTcp()]

            member f.StartActor() =
                actor.Start()
            member f.StopActor() =
                actor.Stop()

            member f.ActorRef = actor.Ref

            interface RemoteActorManager<unit> with
                member f.Init(_, port: int, init: unit) = f.Init(port)
                member f.Fini() = f.Fini()

        type MultipleRepliesStateUTcpManager<'T>() =
            inherit MarshalByRefObject()

            let mutable actor = Actor.sink()

            member f.Init(port: int, initState: 'T) =
                TcpListenerPool.DefaultHostname <- "localhost"
                TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), concurrentAccepts = 10)
                TcpConnectionPool.Init()
                actor <- multipleRepliesStateActor initState
            member f.Fini() =
                actor.Stop()
                TcpConnectionPool.Fini()
                TcpListenerPool.Clear()

            member f.PublishActor() =
                actor <- actor |> Actor.publish [Unidirectional.UTcp()]
            member f.PublishActor(name: string) =
                actor <- actor
                         |> Actor.rename name
                         |> Actor.publish [Unidirectional.UTcp()]

            member f.StartActor() =
                actor.Start()
            member f.StopActor() =
                actor.Stop()

            member f.ActorRef = actor.Ref

            interface RemoteActorManager<'T> with
                member f.Init(_, port: int, init: 'T) = f.Init(port, init)
                member f.Fini() = f.Fini()

        type MultipleRepliesStateBTcpManager<'T>() =
            inherit MarshalByRefObject()

            let mutable actor = Actor.sink()

            member f.Init(port: int, initState: 'T) =
                TcpListenerPool.DefaultHostname <- "localhost"
                TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), concurrentAccepts = 10)
                TcpConnectionPool.Init()
                actor <- multipleRepliesStateActor initState
            member f.Fini() =
                actor.Stop()
                TcpConnectionPool.Fini()
                TcpListenerPool.Clear()

            member f.PublishActor() =
                actor <- actor |> Actor.publish [Bidirectional.BTcp()]
            member f.PublishActor(name: string) =
                actor <- actor
                         |> Actor.rename name
                         |> Actor.publish [Bidirectional.BTcp()]

            member f.StartActor() =
                actor.Start()
            member f.StopActor() =
                actor.Stop()

            member f.ActorRef = actor.Ref

            interface RemoteActorManager<'T> with
                member f.Init(_, port: int, init: 'T) = f.Init(port, init)
                member f.Fini() = f.Fini()

        type RemoteDomainManager<'T, 'U when 'T :> RemoteActorManager<'U>>(port: int, init: 'U, ?appDomainName: string) =
            let appDomainName = defaultArg appDomainName "testDomain"
            let appDomain =
                let o = Console.Out
                let r = Console.IsOutputRedirected

                let currentDomain = AppDomain.CurrentDomain
                let appDomainSetup = new AppDomainSetup()
                appDomainSetup.ApplicationBase <- currentDomain.BaseDirectory
                let evidence = new Security.Policy.Evidence(currentDomain.Evidence)
                printfn "Creating domain..."
                let a = AppDomain.CreateDomain(appDomainName, evidence, appDomainSetup)
                printfn "%A" a
                a

            let remoteActorManager = 
                appDomain.CreateInstance(System.Reflection.Assembly.GetExecutingAssembly().FullName, typeof<'T>.FullName).Unwrap() |> unbox<'T>

            do 
                printfn "remoteActorManager: %A" remoteActorManager
                remoteActorManager.Init(Console.Out, port, init)

            member m.RemoteActorManager = remoteActorManager

            interface IDisposable with
                member m.Dispose() = 
                    remoteActorManager.Fini()
                    AppDomain.Unload(appDomain)
