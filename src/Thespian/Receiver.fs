namespace Nessos.Thespian

open System
open System.Threading

type Receiver<'T>(name : string, protocols : IProtocolServer<'T> []) = 
    inherit Actor<'T>(name, protocols, fun _ -> async.Zero())
    let receiveEvent = new Event<'T>()

    let rec receiveLoop (actor : Actor<'T>) = 
        async { 
            let! msg = actor.Receive()
            receiveEvent.Trigger(msg)
            return! receiveLoop actor
        }

    new(name : string) = new Receiver<'T>(name, [| Actor.DefaultPrimaryProtocolFactory.Create<'T>(name) |])

    member private __.Publish(newProtocolsF : ActorRef<'T> -> IProtocolServer<'T> []) = 
        let mailboxProtocol = new MailboxProtocol.MailboxProtocolServer<_>(name) :> IPrimaryProtocolServer<_>
        let actorRef = new ActorRef<'T>(name, [| mailboxProtocol.Client |])

        let newProtocols = 
            newProtocolsF actorRef
            |> Array.append (protocols
                             |> Seq.map (fun protocol -> protocol.Client.Factory)
                             |> Seq.choose id
                             |> Seq.map (fun factory -> factory.CreateServerInstance<_>(name, actorRef))
                             |> Seq.toArray)
            |> Array.append [| mailboxProtocol |]
        new Receiver<'T>(name, newProtocols) :> Actor<'T>

    member __.ReceiveEvent = receiveEvent.Publish

    override __.Rename(newName : string) = 
        //first check new name
        if newName.Contains("/") then invalidArg "newName" "Receiver names must not contain '/'."
        let mailboxProtocol = new MailboxProtocol.MailboxProtocolServer<_>(newName) :> IPrimaryProtocolServer<_>
        let actorRef = new ActorRef<'T>(newName, [| mailboxProtocol.Client |])

        let newProtocols = 
            protocols
            |> Array.map (fun protocol -> protocol.Client.Factory)
            |> Array.choose id
            |> Array.map (fun factory -> factory.CreateServerInstance(name, actorRef))
            |> Array.append [| mailboxProtocol |]
        new Receiver<'T>(newName, newProtocols) :> Actor<'T>

    override __.Start() = 
        __.ReBind(receiveLoop)
        base.Start()

    override __.Publish(protocols' : IProtocolServer<'T> []) = __.Publish(fun _ -> protocols')
    override __.Publish(protocolFactories : #seq<'U> when 'U :> IProtocolFactory) = 
        __.Publish(fun actorRef -> 
            protocolFactories
            |> Seq.map (fun factory -> factory.CreateServerInstance<'T>(name, actorRef))
            |> Seq.toArray)

[<RequireQualifiedAccess>]
module Receiver = 
    let create<'T>() = new Receiver<'T>(Guid.NewGuid().ToString())
    let rename (name : string) (receiver : Receiver<'T>) : Receiver<'T> = receiver.Rename(name) :?> Receiver<'T>
    let publish (protocolFactories : #seq<'U> when 'U :> IProtocolFactory) (receiver : Receiver<'T>) : Receiver<'T> = 
        receiver.Publish(protocolFactories) :?> Receiver<'T>

    let start (receiver : Receiver<'T>) : Receiver<'T> = 
        receiver.Start()
        receiver

    let toObservable (receiver : Receiver<'T>) : IObservable<'T> = receiver.ReceiveEvent :> IObservable<'T>

    let fromObservable (observable : IObservable<'T>) : Receiver<'T> = 
        let name = Guid.NewGuid().ToString()
        new Receiver<'T>(name, [| new Observable.ObservableProtocolServer<'T>(name, observable) |])

    let forward (actor : Actor<'T>) (receiver : Receiver<'T>) : Actor<'T> = 
        let rec forwardBehavior (self : Actor<'T>) = 
            async { 
                let! msg = self.Receive()
                !actor <-- msg
                return! forwardBehavior self
            }

        let name = Guid.NewGuid().ToString()
        new Actor<'T>(name, [| new Observable.ObservableProtocolServer<'T>(name, receiver.ReceiveEvent) |], 
                      forwardBehavior, [ actor; receiver ])

    module Actor = 
        let bindOnObservable (name : string) (behavior : Actor<'T> -> Async<unit>) (observable : IObservable<'T>) : Actor<'T> = 
            new Actor<'T>(name, [| new Observable.ObservableProtocolServer<'T>(name, observable) |], behavior, [])

        let bindOnReceiver (name : string) (behavior : Actor<'T> -> Async<unit>) (receiver : Receiver<'T>) : Actor<'T> = 
            receiver
            |> toObservable
            |> bindOnObservable name behavior

    module Observable = 
        let toReceiver (observable : IObservable<'T>) : Receiver<'T> = fromObservable observable

        let forward (actor : Actor<'T>) (observable : IObservable<'T>) : Actor<'T> = 
            observable
            |> toReceiver
            |> forward actor
