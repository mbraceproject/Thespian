#I "../../bin/"

#r "Thespian.dll"


open Nessos.Thespian
open Nessos.Thespian.Remote.PipeProtocol

let rec foo state (actor : Actor<int * IReplyChannel<int>>) = async {

    let! msg, rc = actor.Receive()

    printfn "got %d" msg

    rc.Reply <| Value state

    return! foo (state + msg) actor
}

let mkMany n =
    let mkOne i =
        let actor = foo 0 |> Actor.bind |> Actor.rename (string i) |> Actor.publish [ PipeProtocol() ] |> Actor.start
        actor.Ref

    [|1..n|] |> Array.map mkOne

let postMany (actors : ActorRef<'T> []) (msgB: IReplyChannel<'R> -> 'T) =
    actors |> Array.Parallel.map (fun a -> a <!= msgB)
    


let actors = mkMany 20


postMany actors (fun rc -> 1,rc) 