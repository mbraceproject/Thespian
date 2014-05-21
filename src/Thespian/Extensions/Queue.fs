namespace Nessos.Thespian.ActorExtensions

    open System
    open Nessos.Thespian

    [<AutoOpen>]
    module Messages =
        
        type Queue<'T> =
            | Enqueue of 'T
            | DequeueItem of IReplyChannel<'T>
            | Dequeue
            | Peek of IReplyChannel<'T>
            | GetAll of IReplyChannel<'T list>


    module Queue =
        
        let rec queueBehavior (queue: System.Collections.Generic.Queue<'T>) (self: Actor<Queue<'T>>) =
            let rec empty() = 
                self.Scan(
                    function 
                    | Enqueue item ->
                        queue.Enqueue item
                        Some <| notEmpty()
                    | GetAll(R(reply)) ->
                        reply <| Value []
                        Some <| empty() 
                    | _ -> None
                )
            and notEmpty() = async {
                    let! msg = self.Receive()

                    match msg with
                    | Enqueue item ->
                        queue.Enqueue item
                        return! notEmpty()
                    | DequeueItem(R(reply)) ->
                        reply << Value <| queue.Dequeue()
                
                        if queue.Count <> 0 then return! notEmpty()
                        else return! empty()
                    | Dequeue ->
                        queue.Dequeue() |> ignore

                        if queue.Count <> 0 then return! notEmpty()
                        else return! empty()
                    | Peek(R(reply)) ->
                        reply << Value <| queue.Peek()

                        return! notEmpty()
                    | GetAll(R(reply)) ->
                        queue |> Seq.toList |> List.rev |> Value |> reply

                        return! notEmpty()
                }
            empty()
