namespace Thespian.Flow

    open Thespian

    type FlowMailboxActorProtocol<'T>(id: ActorUUID, name: string) =
        inherit MailboxActorProtocol<'T>(id, name)

        let innerActor: Actor<Choice<'T, unit -> unit>> = Actor.bind (fun _ -> async.Zero()) |> Actor.rename (name + "/flowInner")

        let intermediateActor = innerActor.Ref

        member ipc.InnerActor = innerActor

        override ipc.Start(body: unit -> Async<unit>) =
            innerActor.Start()
            base.Start(body)

        interface IActorProtocol<'T> with

            override ipc.Post(msg: 'T) = intermediateActor <-- Choice1Of2(msg)

            override ipc.PostWithReply(msgBuilder: IReplyChannel<'R> -> 'T) = intermediateActor <!- fun ch -> Choice1Of2 (msgBuilder ch)

            override ipc.Start() = 
                invalidOp "Use overload taking actor body."

            override ipc.Stop() = 
                innerActor.Stop()
                ipc.Stop()

