module Nessos.Thespian.Tests.TestDefinitions

open System
open System.Runtime.Serialization
open Nessos.Thespian
open Nessos.Thespian.Tests.AssemblyLoadProxy

type SerializeFailureControl =
    | NeverFail
    | FailOnSerialize
    | FailOnDeserialize of SerializeFailureControl

[<Serializable>]
type ControlledSerialializableException =
    inherit Exception
    val private control: ControlledSerializable

    new (msg: string, control: ControlledSerializable) =
        {
            inherit Exception(msg)
            control = control
        }

    new (info: SerializationInfo, context: StreamingContext) =
        {
            inherit Exception(info, context)
            control = info.GetValue("control", typeof<ControlledSerializable>) :?> ControlledSerializable
        }
    
    member self.Control = self.control

    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            info.AddValue("control", self.control)
            base.GetObjectData(info, context)

and [<Serializable>] ControlledSerializable(control: SerializeFailureControl) =
    new(info: SerializationInfo, context: StreamingContext) = new ControlledSerializable(ControlledSerializable.Deserialize info)

    static member Deserialize(info: SerializationInfo) =
        let control = info.GetValue("control", typeof<SerializeFailureControl>) :?> SerializeFailureControl
        match control with
        | FailOnDeserialize control -> raise <| new ControlledSerialializableException("Deserialization failure", new ControlledSerializable(control))
        | _ -> control    
    
    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            match control with
            | FailOnSerialize -> raise <| new ControlledSerialializableException("Serialization failure", new ControlledSerializable(NeverFail))
            | _ -> info.AddValue("control", control) 
        

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
            | TestSync(rc, _) -> do! rc.Reply ()
        }
    
    let rec consume (self : Actor<TestMessage<unit>>) = 
        async { 
            let! m = self.Receive()
            match m with
            | TestAsync() -> ()
            | TestSync(rc, _) -> do! rc.Reply ()
            return! consume self
        }
    
    let selfStop (self : Actor<TestMessage<unit>>) = 
        async { 
            let! m = self.Receive()
            match m with
            | TestSync(rc, _) -> 
                do! rc.Reply ()
                self.Stop()
            | _ -> self.Stop()
        }
    
    let rec stateful (s : 'S) (self : Actor<TestMessage<'S, 'S>>) = 
        async { 
            let! m = self.Receive()
            match m with
            | TestAsync s' -> return! stateful s' self
            | TestSync(rc, s') -> 
                do! rc.Reply s
                return! stateful s' self
        }

    let rec stateless (self: Actor<TestMessage<'T>>) =
        async {
            let! m = self.Receive()
            match m with
            | TestAsync _ -> return! stateless self
            | TestSync(rc, _) ->
                do! rc.Reply ()
                return! stateless self
        }
    
    let rec failing (self : Actor<TestMessage<'T>>) = 
        async { 
            let! m = self.Receive()
            match m with
            | TestSync(rc, _) -> 
                do! rc.Reply ()
                failwith "Dead sync"
                return! failing self
            | _ -> return! failing self
        }

module Behaviors = 
    let refCell (cell : 'T ref) (m : TestMessage<'T>) = 
        async { 
            match m with
            | TestAsync s -> cell := s
            | TestSync(rc, _) -> do! rc.Reply ()
        }
    
    let state (s : 'S) (m : TestMessage<'S, 'S>) = 
        async { 
            match m with
            | TestAsync s -> return s
            | TestSync(rc, s') -> 
                do! rc.Reply s
                return s'
        }
    
    let stateNoUpdateOnSync (s : 'S) (m : TestMessage<'S, 'S>) = 
        async { 
            match m with
            | TestAsync s -> return s
            | TestSync(rc, _) -> 
                do! rc.Reply s
                return s
        }
    
    let delayedState (s : int) (m : TestMessage<int, int>) = 
        async { 
            match m with
            | TestAsync s -> return s
            | TestSync(rc, t) -> 
                do! Async.Sleep t
                do! rc.Reply s
                return s
        }
    
    let list (l : 'T list) (m : TestList<'T>) = 
        async { 
            match m with
            | ListPrepend v -> return v :: l
            | Delay t -> 
                do! Async.Sleep t
                return l
            | ListGet rc -> 
                do! rc.Reply l
                return l
        }
    
    let adder (i : int) (m : TestMessage<int, int>) = 
        async { 
            match m with
            | TestAsync i' -> return i + i'
            | TestSync(rc, _) -> 
                do! rc.Reply i
                return i
        }
    
    let divider (i : int) (m : TestMessage<int, int>) = 
        async { 
            match m with
            | TestAsync i' -> return i'
            | TestSync(rc, i') -> 
                try 
                    let i'' = i / i'
                    do! rc.Reply i'
                    return i''
                with e -> 
                    do! rc.ReplyWithException e
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
            | MultiRepliesSync(rc1, rc2) -> 
                do! rc1.Reply ()
                do! rc2.Reply s
                return s
        }


module Remote = 
    open System.Reflection
    open Nessos.Thespian.Remote
    
    type BehaviorValue<'T> = 
        | Behavior of byte []
        
        static member Create(behavior : Actor<'T> -> Async<unit>) = 
            let serializer = Serialization.defaultSerializer
            Behavior(serializer.Serialize<Actor<'T> -> Async<unit>>(behavior))
        
        member self.Unwrap() = 
            let (Behavior payload) = self
            let serializer = Serialization.defaultSerializer
            serializer.Deserialize<Actor<'T> -> Async<unit>>(payload)

    type ActorManager() =
        let mutable disposers = []

        member __.Create(behavior : Actor<'T> -> Async<unit>, protocolFactory, ?name : string) =
            let actor = 
                Actor.bind behavior 
                |> fun a -> match name with Some n -> Actor.rename n a | None -> a
                |> Actor.publish [ protocolFactory() ]

            do actor.Start()

            disposers <- actor :> IDisposable :: disposers
            actor.Ref
        
        interface IDisposable with member __.Dispose() = for d in disposers do d.Dispose()
    
#if NETCOREAPP
    [<AutoSerializable(false)>]
    type RemoteActorManager(protocolFactory : unit -> IProtocolFactory) =
        let loadContext = new MirroredAssemblyLoadContext()
        let managerProxy = loadContext.CreateProxy<ActorManager>()

        member __.CreateActor(behaviour, ?name) =
            let factory = protocolFactory
            managerProxy.Execute (fun mgr -> async { return mgr.Create(behaviour, factory, ?name = name)})
            |> Async.RunSynchronously

        interface IDisposable with
            member __.Dispose() = managerProxy.Dispose() ; (loadContext :> IDisposable).Dispose()

    type RemoteUtcpActorManager() =
        inherit RemoteActorManager(Protocols.utcp)

    type RemoteBtcpActorManager() =
        inherit RemoteActorManager(Protocols.btcp)
#else
    type ActorManagerProxy(protocolFactory) =
        inherit MarshalByRefObject()

        let manager = new ActorManager()

        member __.CreateActor(behavior : Actor<'T> -> Async<unit>, ?name : string) = manager.Create(behavior, protocolFactory, ?name = name)
        member __.Fini() = (manager :> IDisposable).Dispose()
        override __.InitializeLifetimeService() = null

    type BTcpManagerProxy() =
        inherit ActorManagerProxy(Protocols.btcp)

    type UTcpManagerProxy() =
        inherit ActorManagerProxy(Protocols.utcp)

    type RemoteActorManager(proxy : ActorManagerProxy) =
        member __.CreateActor(behaviour, ?name) = proxy.CreateActor(behaviour, ?name = name)
        interface IDisposable with member __.Dispose() = proxy.Fini()

    type RemoteUtcpActorManager() =
        inherit RemoteActorManager(AppDomain.CreateNew("utcpDomain").CreateInstance<UTcpManagerProxy>())

    type RemoteBtcpActorManager() =
        inherit RemoteActorManager(AppDomain.CreateNew("btcpDomain").CreateInstance<UTcpManagerProxy>())

#endif

module Assert =
    open NUnit.Framework

    let throws<'e when 'e :> exn> (f : unit -> unit) = Assert.Throws<'e>(fun () -> f ()) |> ignore