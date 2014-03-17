namespace Nessos.Thespian.Flow
    
    open System

    open Nessos.Thespian

    type Actor<'T> private (id: ActorUUID, name: string, protocols: IActorProtocol<'T>[], body: Actor<'T> -> Async<unit>) =
        inherit Nessos.Thespian.Actor<'T>(id, name, protocols, fun actor -> body (actor :?> Actor<_>))

        let innerActor = (protocols.[0] :?> FlowMailboxActorProtocol<'T>).InnerActor
        let innerActorRef = innerActor.Ref

        new(name: string, body: Actor<'T> -> Async<unit>) = 
            let id = Guid.NewGuid()
            new Actor<'T>(id, name, [| new FlowMailboxActorProtocol<_>(id, name) |], body)
        
        new(body: Actor<'T> -> Async<unit>) = new Actor<'T>(System.Guid.NewGuid().ToString(), body)

        override actor.Publish(protocols': IActorProtocol<'T>[]) = 
            new Actor<'T>(id, name, protocols' |> Array.append protocols, body) :> Nessos.Thespian.Actor<'T>

        override actor.Receive() = async {
                let! res = innerActor.Receive()

                return! match res with 
                        | Choice1Of2 msg -> async.Return msg
                        | Choice2Of2 k -> 
                            k() 
                            actor.Receive()
            }

        member actor.FlowStart (reply: Reply<'R> -> unit) (flowComputation: Flow<'R>) =
            let (Flow f) = flowComputation in f actor.Ref reply (fun k -> innerActorRef <-- Choice2Of2 k)

    [<AutoOpen>]
    module Operators =
        
        let flow = new FlowBuilder()

        let (<!~) (actor: ActorRef<'T>) (msgBuilder: IReplyChannel<'R> -> 'T): Flow<'R> =
            Flow(fun a k sub ->
                async {
                    try
                        //decorate the reply channel
                        let! res = actor <!- msgBuilder

                        sub (fun () -> k <| Value res)

                    with _ as ex ->
                        k <| Exception ex
                } |> Async.Start
            )

    module Actor =
        let bind (name: string) (body: Actor<'T> -> Async<unit>) =
            new Actor<'T>(name, body)

