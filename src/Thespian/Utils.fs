namespace Thespian.Utils

    open System
    open System.Threading

    [<AutoOpen>]
    module Utils =
        
        open Thespian

        let memoize f =
            let cache = Atom Map.empty

            fun id ->
                match cache.Value.TryFind id with
                | None ->
                    let result = f id
                    cache.Swap (fun c -> c.Add(id, result))
                    result
                | Some result -> result

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
        
        //BASED ON http://blogs.msdn.com/b/chrsmith/archive/2008/02/22/regular-expressions-via-active-patterns.aspx

        open System
        open System.Text.RegularExpressions

        let (|Match|_|) =
            // eirik : greatly improve performance by memoizing compiled regex objects
            let regex = memoize(fun pattern -> Regex(pattern))
            
            fun (pat : string) (inp : string) ->
                let m = (regex pat).Match inp in
                if m.Success 
                then Some (List.tail [ for g in m.Groups -> g.Value ])
                else None
