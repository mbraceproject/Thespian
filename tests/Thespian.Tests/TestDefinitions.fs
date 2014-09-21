module Nessos.Thespian.Tests.TestDefinitions

open System
open Nessos.Thespian

type TestMessage<'T, 'R> = 
    | TestAsync of 'T
    | TestSync of IReplyChannel<'R> * 'T

type TestMessage<'T> = TestMessage<'T, unit>

type TestList<'T> = 
    | ListPrepend of 'T
    | Delay of int
    | ListGet of IReplyChannel<'T list>

type TestMultiReplies<'T> = 
    | MultiRepliesAsync of 'T
    | MultiRepliesSync of IReplyChannel<unit> * IReplyChannel<'T>

module PrimitiveBehaviors = 
    let nill (self : Actor<TestMessage<unit>>) = async.Zero()
    
    let consumeOne (self : Actor<TestMessage<'T>>) = 
        async { 
            let! m = self.Receive()
            match m with
            | TestAsync _ -> ()
            | TestSync(R reply, _) -> reply <| Ok ()
        }
    
    let rec consume (self : Actor<TestMessage<unit>>) = 
        async { 
            let! m = self.Receive()
            match m with
            | TestAsync() -> ()
            | TestSync(R reply, _) -> reply <| Ok ()
            return! consume self
        }
    
    let selfStop (self : Actor<TestMessage<unit>>) = 
        async { 
            let! m = self.Receive()
            match m with
            | TestSync(R reply, _) -> 
                reply <| Ok ()
                self.Stop()
            | _ -> self.Stop()
        }
    
    let rec stateful (s : 'S) (self : Actor<TestMessage<'S, 'S>>) = 
        async { 
            let! m = self.Receive()
            match m with
            | TestAsync s' -> return! stateful s' self
            | TestSync(R reply, s') -> 
                reply (Ok s)
                return! stateful s' self
        }
    
    let rec failing (self : Actor<TestMessage<'T>>) = 
        async { 
            let! m = self.Receive()
            match m with
            | TestSync(R reply, _) -> 
                reply nothing
                failwith "Dead sync"
                return! failing self
            | _ -> return! failing self
        }

module Behaviors = 
    let refCell (cell : 'T ref) (m : TestMessage<'T>) = 
        async { 
            match m with
            | TestAsync s -> cell := s
            | TestSync(R reply, _) -> reply nothing
        }
    
    let state (s : 'S) (m : TestMessage<'S, 'S>) = 
        async { 
            match m with
            | TestAsync s -> return s
            | TestSync(R reply, s') -> 
                reply (Ok s)
                return s'
        }
    
    let stateNoUpdateOnSync (s : 'S) (m : TestMessage<'S, 'S>) = 
        async { 
            match m with
            | TestAsync s -> return s
            | TestSync(R reply, _) -> 
                reply (Ok s)
                return s
        }
    
    let delayedState (s : int) (m : TestMessage<int, int>) = 
        async { 
            match m with
            | TestAsync s -> return s
            | TestSync(R reply, t) -> 
                do! Async.Sleep t
                reply <| Ok s
                return s
        }
    
    let list (l : 'T list) (m : TestList<'T>) = 
        async { 
            match m with
            | ListPrepend v -> return v :: l
            | Delay t -> 
                do! Async.Sleep t
                return l
            | ListGet(R reply) -> 
                reply <| Ok l
                return l
        }
    
    let adder (i : int) (m : TestMessage<int, int>) = 
        async { 
            match m with
            | TestAsync i' -> return i + i'
            | TestSync(R reply, _) -> 
                reply <| Ok i
                return i
        }
    
    let divider (i : int) (m : TestMessage<int, int>) = 
        async { 
            match m with
            | TestAsync i' -> return i'
            | TestSync(R reply, i') -> 
                try 
                    let i'' = i / i'
                    reply <| Ok i'
                    return i''
                with e -> 
                    reply (Exn e)
                    return i
        }
    
    let forward (target : ActorRef<'T>) (m : 'T) = target <-!- m
    
    let multiRepliesForward (target : ActorRef<TestMultiReplies<'T>>) (m : TestMessage<'T, 'T>) = 
        async { 
            match m with
            | TestAsync v -> do! target <-!- MultiRepliesAsync v
            | TestSync(rc, _) -> do! target <!- fun ch -> MultiRepliesSync(ch, rc)
        }
    
    let multiRepliesState (s : 'S) (m : TestMultiReplies<'S>) = 
        async { 
            match m with
            | MultiRepliesAsync v -> return v
            | MultiRepliesSync(R reply1, R reply2) -> 
                reply1 nothing
                reply2 <| Ok s
                return s
        }


module Remote = 
    open System.Reflection
    open Nessos.Thespian.Remote
    open Nessos.Thespian.Remote.PipeProtocol
    
    [<AbstractClass>]
    type ActorManager() = 
        inherit MarshalByRefObject()
        abstract Init : unit -> unit
        abstract Fini : unit -> unit
        override __.Init() = ()
        override __.Fini() = ()
    
    [<AbstractClass>]
    type ActorManager<'T>(behavior : Actor<'T> -> Async<unit>, ?name : string) = 
        inherit ActorManager()
        
        [<VolatileField>]
        let mutable actor = 
            Actor.bind behavior |> fun a -> 
                if name.IsSome then Actor.rename name.Value a
                else a
        
        abstract Actor : Actor<'T>
        abstract SetActor : Actor<'T> -> unit
        abstract Ref : ActorRef<'T>
        abstract Publish : unit -> ActorRef<'T>
        abstract Start : unit -> unit
        abstract Stop : unit -> unit
        override __.Actor = actor
        override __.SetActor(actor' : Actor<'T>) = actor <- actor'
        override __.Ref = actor.Ref
        override __.Start() = actor.Start()
        override __.Stop() = actor.Stop()
        override __.Fini() = __.Stop()
    
    type UtcpActorManager<'T>(behavior : Actor<'T> -> Async<unit>, ?name : string) = 
        inherit ActorManager<'T>(behavior, ?name = name)
        override self.Publish() = 
            let actor = self.Actor |> Actor.publish [ Protocols.utcp() ]
            self.SetActor(actor)
            actor.Ref
    
    type BtcpActorManager<'T>(behavior : Actor<'T> -> Async<unit>, ?name : string) = 
        inherit ActorManager<'T>(behavior, ?name = name)
        override self.Publish() = 
            let actor = self.Actor |> Actor.publish [ Protocols.btcp() ]
            self.SetActor(actor)
            actor.Ref
    
    type NppActorManager<'T>(behavior : Actor<'T> -> Async<unit>, ?name : string) = 
        inherit ActorManager<'T>(behavior, ?name = name)
        override self.Publish() = 
            let actor = self.Actor |> Actor.publish [ Protocols.npp() ]
            self.SetActor(actor)
            actor.Ref
    
    type BehaviorValue<'T> = 
        | Behavior of byte []
        
        static member Create(behavior : Actor<'T> -> Async<unit>) = 
            let serializer = Serialization.defaultSerializer
            Behavior(serializer.Serialize<Actor<'T> -> Async<unit>>(behavior))
        
        member self.Unwrap() = 
            let (Behavior payload) = self
            let serializer = Serialization.defaultSerializer
            serializer.Deserialize<Actor<'T> -> Async<unit>>(payload)
    
    [<AbstractClass>]
    type ActorManagerFactory() = 
        inherit MarshalByRefObject()
        abstract CreateActorManager : (Actor<'T> -> Async<unit>) * ?name:string -> ActorManager<'T>
        abstract Fini : unit -> unit
    
    type UtcpActorManagerFactory() = 
        inherit ActorManagerFactory()
        let mutable managers = []
        
        override __.CreateActorManager(behavior : Actor<'T> -> Async<unit>, ?name : string) = 
            let manager = new UtcpActorManager<'T>(behavior, ?name = name)
            manager.Init()
            managers <- (manager :> ActorManager) :: managers
            manager :> ActorManager<'T>
        
        override __.Fini() = 
            for manager in managers do
                manager.Fini()
    
    type BtcpActorManagerFactory() = 
        inherit ActorManagerFactory()
        let mutable managers = []
        
        override __.CreateActorManager(behavior : Actor<'T> -> Async<unit>, ?name : string) = 
            let manager = new BtcpActorManager<'T>(behavior, ?name = name)
            manager.Init()
            managers <- (manager :> ActorManager) :: managers
            manager :> ActorManager<'T>
        
        override __.Fini() = 
            for manager in managers do
                manager.Fini()
    
    type NppActorManagerFactory() = 
        inherit ActorManagerFactory()
        let mutable managers = []
        
        override __.CreateActorManager(behavior : Actor<'T> -> Async<unit>, ?name : string) = 
            let manager = new NppActorManager<'T>(behavior, ?name = name)
            manager.Init()
            managers <- (manager :> ActorManager) :: managers
            manager :> ActorManager<'T>
        
        override __.Fini() = 
            for manager in managers do
                manager.Fini()
    
    open System.Collections.Generic
    
    type AppDomainPool() = 
        static let appDomains = new Dictionary<string, AppDomain>()
        
        static member CreateDomain(name : string) = 
            //      printfn "Creating domain: %s" name
            let currentDomain = AppDomain.CurrentDomain
            let appDomainSetup = currentDomain.SetupInformation
            let evidence = new Security.Policy.Evidence(currentDomain.Evidence)
            let a = AppDomain.CreateDomain(name, evidence, appDomainSetup)
            appDomains.Add(name, a)
            a
        
        static member GetOrCreate(name : string) = 
            let exists, appdomain = appDomains.TryGetValue(name)
            if exists then appdomain
            else AppDomainPool.CreateDomain(name)
    
    type AppDomainManager<'T when 'T :> ActorManagerFactory>(?appDomainName : string) = 
        let appDomainName = defaultArg appDomainName "testDomain"
        let appDomain = AppDomainPool.GetOrCreate(appDomainName)
        let factory = 
            appDomain.CreateInstance(Assembly.GetExecutingAssembly().FullName, typeof<'T>.FullName).Unwrap() 
            |> unbox<'T>
        member __.Factory = factory
        interface IDisposable with
            member __.Dispose() = factory.Fini()

