namespace Nessos.Thespian.Concurrency

open System.Threading

type Atom<'T when 'T : not struct>(value : 'T) =
    let refCell = ref value

    let rec swap f = 
        let currentValue = !refCell
        let result = Interlocked.CompareExchange<'T>(refCell, f currentValue, currentValue)
        if obj.ReferenceEquals(result, currentValue) then ()
        else Thread.SpinWait 20; swap f

    let transact f =
        let result = ref Unchecked.defaultof<_>
        let f' t = let t',r = f t in result := r ; t'
        swap f' ; result.Value

    member self.Value with get() : 'T = !refCell
    member self.Swap (f : 'T -> 'T) : unit = swap f
    member self.Transact(f : 'T -> 'T * 'R) : 'R = transact f
    member self.Set (t : 'T) = swap (fun _ -> t)


[<RequireQualifiedAccess>]
module Atom =

    let atom<'T when 'T : not struct> value = new Atom<'T>(value)
    let swap (atom : Atom<'T>) f = atom.Swap f
    let transact (atom : Atom<'T>) f : 'R = atom.Transact f
    let set (atom : Atom<'T>) t = atom.Set t


type Latch() =
    let mutable switch = 0
    member __.Trigger() = Interlocked.CompareExchange(&switch, 1, 0) = 0

/// thread safe counter implementation
type ConcurrentCounter (?start : int64) =
    let count = ref <| defaultArg start 0L

    member __.Incr () = System.Threading.Interlocked.Increment count
    member __.Value = count

type CountdownLatch() =
    [<VolatileField>]
    let mutable counter = 0

    ///Set the latch
    member self.Increment(): unit =
        Interlocked.Increment(&counter) |> ignore

    ///Reset the latch
    member __.Decrement(): unit =
        Interlocked.Decrement(&counter) |> ignore

    ///Spin-wait until the latch is reset
    member __.WaitToZero(): unit =
        while (Interlocked.CompareExchange(&counter, 0, 0) <> 0) do Thread.SpinWait 20
