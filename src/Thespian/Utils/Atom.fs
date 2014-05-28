namespace Nessos.Thespian

    open System
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

        let atom value = new Atom<'T>(value)
        let swap (atom : Atom<'T>) f = atom.Swap f
        let transact (atom : Atom<'T>) f : 'R = atom.Transact f
        let set (atom : Atom<'T>) t = atom.Set t