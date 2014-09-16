module internal Nessos.Thespian.LazyList

    /// LazyList implementation
    type LazyList<'T> =
        private
        | Empty
        | Tail of Lazy<LazyList<'T>>
        | NonEmpty of 'T * LazyList<'T>
        
    [<RequireQualifiedAccess>]
    module LazyList =
        /// <summary>
        /// Nil/Cons active pattern.
        /// </summary>
        /// <param name="ll"></param>
        let rec (|Nil|Cons|) (ll : LazyList<'T>) =
            match ll with
            | Empty -> Nil
            | Tail lt -> (|Nil|Cons|) lt.Value
            | NonEmpty(t,tl) -> Cons(t, tl)
            

        /// <summary>
        /// Build a lazy list out of IEnumerable.
        /// </summary>
        /// <param name="xs"></param>
        let ofSeq (xs : 'T seq) =
            let e = xs.GetEnumerator()
            let rec unfold () = lazy(
                if e.MoveNext() then
                    NonEmpty(e.Current, Tail(unfold()))
                else Empty )

            Tail(unfold ())
