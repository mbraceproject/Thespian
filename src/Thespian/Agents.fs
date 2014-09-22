namespace Nessos.Thespian.Agents

    open System
    open Nessos.Thespian

    type private AgentMessage<'T> =
        | Swap of ('T -> 'T)
        | SafeSwap of IReplyChannel<unit> * ('T -> 'T)
        | Read of IReplyChannel<'T>

    type Agent<'T>(init: 'T) =
        let refCell = ref init

        let safeSwap swap =
            let currentValue = refCell.Value
            try refCell := swap refCell.Value with _ -> refCell := currentValue; reraise()

        let unsafeSwap swap = 
            let currentValue = refCell.Value
            try refCell := swap refCell.Value with _ -> refCell := currentValue

        let agentBehavior (msg: AgentMessage<'T>) =
            async {
                match msg with
                | Swap swap -> unsafeSwap swap
                | SafeSwap(rc, swap) -> try safeSwap swap; do! rc.Reply () with e -> do! rc.ReplyWithException e
                | Read(rc) -> do! rc.Reply refCell.Value
            }
    
        let actor = Actor.bind <| Behavior.stateless agentBehavior

        do
            actor.Start()

        member agent.Value with get() = refCell.Value

        member agent.Send(action: 'T -> 'T) = !actor <-- Swap action
        member agent.SendSafe(action: 'T -> 'T) = try !actor <!= fun ch -> SafeSwap(ch, action) with MessageHandlingException(_, e) | e -> raise e

        member agent.ReadAsync() = !actor <!- Read
        member agent.Read() = agent.ReadAsync() |> Async.RunSynchronously

        interface IDisposable with
            member agent.Dispose() =
                (actor :> IDisposable).Dispose()

    module Agent =
        let start (state: 'T) = new Agent<_>(state)

        let send (action: 'T -> 'T) (agent: Agent<'T>) = agent.Send action
        let sendSafe (action: 'T -> 'T) (agent: Agent<'T>) = agent.SendSafe action

    [<AutoOpen>]
    module Operators =
        let (~%) (agent: Agent<'T>) = agent.Value

        let (!%) (agent: Agent<'T>) = agent.ReadAsync()
        let (!!%) (agent: Agent<'T>) = agent.Read()

        let (<%-) (agent: Agent<'T>) (swapF: 'T -> 'T) = agent.Send swapF

        let (%=) (agent: Agent<'T>) (value: 'T) = agent <%- fun _ -> value
