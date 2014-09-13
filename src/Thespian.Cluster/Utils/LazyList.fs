module internal Nessos.Thespian.LazyList
    
    type LazyList<'T> =
        private
        | Empty
        | Tail of Lazy<LazyList<'T>>
        | NonEmpty of 'T * LazyList<'T>
        
    [<RequireQualifiedAccess>]
    module LazyList =
        let rec (|Nil|Cons|) (ll : LazyList<'T>) =
            match ll with
            | Empty -> Nil
            | Tail lt -> (|Nil|Cons|) lt.Value
            | NonEmpty(t,tl) -> Cons(t, tl)
            
            
        let ofSeq (xs : 'T seq) =
            let e = xs.GetEnumerator()
            let rec unfold () = lazy(
                if e.MoveNext() then
                    NonEmpty(e.Current, Tail(unfold()))
                else Empty )

            Tail(unfold ())