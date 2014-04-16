namespace Nessos.Thespian.Utils

    open System
    open System.Threading
    open System.Collections.Concurrent

    open Nessos.Thespian

    [<AutoOpen>]
    module Utils =

        let memoize f =
            let cache = new ConcurrentDictionary<_,_>()

            fun x ->
                let found, y = cache.TryGetValue x
                if found then y
                else
                    let y = f x
                    let _ = cache.TryAdd(x, y)
                    y

        let compareOn (f: 'T -> 'U when 'U : comparison) (x: 'T) (other: obj): int =
            match other with
            | :? 'T as y -> compare (f x) (f y)
            | _ -> invalidArg "other" "Unable to compare values of incompatible types."

        let hashOn (f: 'T -> 'U when 'U : equality) (x: 'T): int = hash (f x)

        let equalsOn (f: 'T -> 'U when 'U : equality) (x: 'T) (other: obj): bool =
            match other with
            | :? 'T as y -> (f x = f y)
            | _ -> false

        let equalsOnComparison (f: 'T -> 'U when 'U : comparison) (x: 'T) (other: obj): bool =
            match other with
            | :? 'T as y -> compareOn f x y = 0
            | _ -> false

    module RegExp =

        open System
        open System.Text.RegularExpressions

        let (|Match|_|) =
            let regex = memoize(fun pattern -> Regex(pattern))
            
            fun (pat : string) (inp : string) ->
                let m = (regex pat).Match inp in
                if m.Success 
                then Some (List.tail [ for g in m.Groups -> g.Value ])
                else None
