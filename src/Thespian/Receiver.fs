namespace Nessos.Thespian

    open System
    open System.Threading

    type Receiver<'T> internal (uuid: ActorUUID, name: string, protocols: IActorProtocol<'T>[]) =
        inherit Actor<'T>(uuid, name, protocols, fun _ -> async.Zero())

        let receiveEvent = new Event<'T>()

        let rec receiveLoop (actor: Actor<'T>) = async {
            let! msg = actor.Receive()

            receiveEvent.Trigger(msg)

            return! receiveLoop actor
        }

        new (name: string) = 
            let id = Guid.NewGuid()

            new Receiver<'T>(id, name, [| new MailboxActorProtocol<'T>(id, name) |])

        member private actor.Publish(newProtocolsF: ActorRef<'T> -> IActorProtocol<'T>[]) =
            let newId = ActorUUID.NewGuid()
            let mailboxProtocol = new MailboxActorProtocol<_>(newId, name)
            let actorRef = new ActorRef<'T>(newId, name, [| mailboxProtocol |])
            let newProtocols = newProtocolsF actorRef
                               |> Array.append (protocols |> Seq.map (fun protocol -> protocol.Configuration) 
                                                          |> Seq.choose id |> Seq.collect (fun conf -> conf.CreateProtocolInstances(actorRef)) 
                                                          |> Seq.toArray)
                               |> Array.append [| mailboxProtocol |]

            new Receiver<'T>(newId, name, newProtocols) :> Actor<'T>

        member r.ReceiveEvent = receiveEvent.Publish

        override r.Rename(newName: string) =
            //first check new name
            if newName.Contains("/") then invalidArg "newName" "Receiver names must not contain '/'."

            let newId = ActorUUID.NewGuid()
            let mailboxProtocol = new MailboxActorProtocol<_>(newId, newName)
            let actorRef = new ActorRef<'T>(newId, newName, [| mailboxProtocol |])

            let newProtocols = protocols |> Array.map (fun protocol -> protocol.Configuration)
                                         |> Array.choose id
                                         |> Array.collect (fun conf -> conf.CreateProtocolInstances(actorRef))
                                         |> Array.append [| mailboxProtocol |]
            
            new Receiver<'T>(newId, newName, newProtocols) :> Actor<'T>

        override r.Start() =
            r.ReBind(receiveLoop)

        override r.Publish(protocols': IActorProtocol<'T>[]) = 
            r.Publish(fun _ -> protocols')

        override r.Publish(configurations: #seq<'U> when 'U :> IProtocolConfiguration) =
            r.Publish(fun actorRef -> configurations |> Seq.collect (fun conf -> conf.CreateProtocolInstances(actorRef)) |> Seq.toArray)
        
    type ObservableActorProtocol<'T>(actorUUId: ActorUUID, actorName: string, observable: IObservable<'T>) as self =
        let observationActor = Actor.empty()

        [<VolatileField>]
        let mutable remover: IDisposable option = None

        [<VolatileField>]
        let mutable cancellationTokenSource = new CancellationTokenSource()

        let protocol = self :> IPrincipalActorProtocol<'T>

        interface IPrincipalActorProtocol<'T> with

            member op.ProtocolName = "observable"
            member op.Configuration = None
            member op.MessageType = typeof<'T>
            member op.ActorUUId = actorUUId
            member op.ActorName = actorName
            member op.ActorId = new MailboxActorId(actorUUId, actorName) :> ActorId
            
            member op.CurrentQueueLength with get() = observationActor.CurrentQueueLength

            member op.Log = observationActor.Log

            member op.Start() =
                raise <| new InvalidOperationException("Principal Protocol; Use the overload that requires the actor's behavior.")

            member op.Start(body: unit -> Async<unit>) =
                match remover with
                | None ->
                    observationActor.Start()
                    Async.Start(body(), cancellationTokenSource.Token)
                    remover <- Some( observable.Subscribe (!observationActor).Post )
                | Some _ -> ()

            member op.Stop() =
                match remover with
                | Some disposable ->
                    observationActor.Stop()
                    cancellationTokenSource.Cancel()
                    cancellationTokenSource <- new CancellationTokenSource()
                    disposable.Dispose()
                    remover <- None
                | None -> ()

            member op.Receive(timeout: int) = observationActor.Receive(timeout)

            member op.Receive() = protocol.Receive(-1)

            member op.TryReceive(timeout: int) = observationActor.TryReceive(timeout)

            member op.TryReceive() = protocol.TryReceive(-1)

            member op.Scan(scanner: 'T -> Async<'U> option, timeout: int): Async<'U> =
                observationActor.Scan(scanner, timeout)

            member op.Scan(scanner: 'T -> Async<'U> option) = protocol.Scan(scanner, -1)

            member op.TryScan(scanner: 'T -> Async<'U> option, timeout: int): Async<'U option> =
                observationActor.TryScan(scanner, timeout)

            member op.TryScan(scanner: 'T -> Async<'U> option) = protocol.TryScan(scanner, -1)

            member op.Post(msg: 'T) = !observationActor <-- msg
            member op.PostAsync(msg: 'T) = observationActor.Ref.PostAsync(msg)

            member op.PostWithReply<'R>(msgBuilder: IReplyChannel<'R> -> 'T): Async<'R> =
                !observationActor <!- msgBuilder

            member op.PostWithReply<'R>(msgBuilder: IReplyChannel<'R> -> 'T, timeout: int): Async<'R> =
                observationActor.Ref.PostWithReply(msgBuilder, timeout)

            member op.TryPostWithReply<'R>(msgBuilder: IReplyChannel<'R> -> 'T, timeout: int): Async<'R option> =
                observationActor.Ref.TryPostWithReply(msgBuilder, timeout)


    module Receiver =
        let create<'T> () = new Receiver<'T>("")

        let rename (name: string) (receiver: Receiver<'T>): Receiver<'T> =
            receiver.Rename(name) :?> Receiver<'T>

        let publish (protocolConfs: #seq<'U> when 'U :> IProtocolConfiguration) (receiver: Receiver<'T>): Receiver<'T> =
            receiver.Publish(protocolConfs) :?> Receiver<'T>

        let start (receiver: Receiver<'T>): Receiver<'T> =
            receiver.Start()
            receiver

        let toObservable (receiver: Receiver<'T>): IObservable<'T> =
            receiver.ReceiveEvent :> IObservable<'T>

        let fromObservable (observable: IObservable<'T>): Receiver<'T> =
            let uuid = Guid.NewGuid()

            new Receiver<'T>(uuid, "", [| new ObservableActorProtocol<'T>(uuid, "", observable) |])

        let forward (actor: Actor<'T>) (receiver: Receiver<'T>): Actor<'T> =
            let rec forwardBehavior (self: Actor<'T>) = async {
                let! msg = self.Receive()

                !actor <-- msg

                return! forwardBehavior self
            }
            
            let uuid = Guid.NewGuid()

            new Actor<'T>(uuid, "", [| new ObservableActorProtocol<'T>(uuid, "", receiver.ReceiveEvent) |], forwardBehavior, [actor; receiver])

        module Actor =
            let bindOnObservable (name: string) (behavior: Actor<'T> -> Async<unit>) (observable: IObservable<'T>): Actor<'T> =
                let uuid = Guid.NewGuid()

                new Actor<'T>(uuid, name, [| new ObservableActorProtocol<'T>(uuid, "", observable) |], behavior)

            let bindOnReceiver (name: string) (behavior: Actor<'T> -> Async<unit>) (receiver: Receiver<'T>): Actor<'T> =
                receiver |> toObservable |> bindOnObservable name behavior


        module Observable = 
            let toReceiver (observable: IObservable<'T>): Receiver<'T> =
                fromObservable observable

            let forward (actor: Actor<'T>) (observable: IObservable<'T>): Actor<'T> =
                observable |> toReceiver |> forward actor
        
