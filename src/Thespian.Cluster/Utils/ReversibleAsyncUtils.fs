namespace Nessos.Thespian.Reversible

    open System
    open Nessos.Thespian.Reversible
        
    [<AutoOpen>]
    module RevAsyncUtils =
        [<RequireQualifiedAccess>]
        module RevAsync =
            /// postcompose covariant operation
            let map (f : 'T -> 'S) (w : RevAsync<'T>) : RevAsync<'S> =
                revasync { let! r = w in return f r }

            /// lifting of lambdas to revasync funcs
            let lift (f : 'T -> 'S) = fun t -> revasync { return f t }
        

            /// revasync failwith
            let rafailwith msg = RevAsync.Raise(System.Exception msg) : RevAsync<'T>
            /// revasync failwithf
            let rafailwithf fmt = Printf.ksprintf rafailwith fmt : RevAsync<'T>

        module List =
            let rec foldRevAsync (foldF: 'U -> 'T -> RevAsync<'U>) (state: 'U) (items: 'T list): RevAsync<'U> =
                revasync {
                    match items with
                    | [] -> return state
                    | item::rest ->
                        let! nextState = foldF state item
                        return! foldRevAsync foldF nextState rest
                }

            let foldBackRevAsync (foldF: 'T -> 'U -> RevAsync<'U>) (items: 'T list) (state: 'U): RevAsync<'U> =
                let rec loop is k = revasync {
                    match is with
                    | [] -> return! k state
                    | h::t -> return! loop t (fun acc -> revasync { let! acc' = foldF h acc in return! k acc' })
                }

                loop items revasync.Return

            let mapRevAsync (mapF: 'T -> RevAsync<'U>) (items: 'T list): RevAsync<'U list> =
                foldRevAsync (fun is i -> revasync { let! i' = mapF i in return i'::is }) [] items

            let chooseRevAsync (choiceF: 'T -> RevAsync<'U option>) (items: 'T list): RevAsync<'U list> =
                foldRevAsync (fun is i -> revasync { let! r = choiceF i in return match r with Some i' -> i'::is | _ -> is }) [] items
