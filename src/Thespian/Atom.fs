namespace Thespian

    open System
    open System.Threading

    type Atom<'T when 'T : not struct>(value : 'T) =
        let refCell = ref value
    
        let rec swap f = 
            let currentValue = !refCell
            let result = Interlocked.CompareExchange<'T>(refCell, f currentValue, currentValue)
            if obj.ReferenceEquals(result, currentValue) then ()
            else Thread.SpinWait 20; swap f
        
        member self.Value with get() : 'T = !refCell
        member self.Swap (f : 'T -> 'T) : unit = swap f


    module Atom =

        // val atom : 'T -> Atom<'T> when 'T : not struct
        let atom value = 
            new Atom<_>(value)
        
        // val ( ! ) : Atom<'T> -> 'T when 'T : not struct
        let (!) (atom : Atom<_>) =  
            atom.Value
    
        // val swap : Atom<'T> -> ('T -> 'T) -> unit when 'T : not struct
        let swap (atom : Atom<_>) (f : _ -> _) =
            atom.Swap f

        // val ( |AtomCell| ) : Atom<'T> -> 'T
        let (|AtomCell|) (atomCell : Atom<'T>) = !atomCell

