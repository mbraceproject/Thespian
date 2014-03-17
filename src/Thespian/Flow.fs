namespace Nessos.Thespian.Flow

    open Nessos.Thespian

    //Flow is a continuation monad
    
    type Cont<'T> = Reply<'T> -> unit
    type Subscribe = (unit -> unit) -> unit

    type Flow<'T> = Flow of (ActorRef -> Cont<'T> -> Subscribe -> unit)
    
    type FlowBuilder() =
        member flow.Bind(computation: Flow<'T>, bindFunc: 'T -> Flow<'U>): Flow<'U> =
            Flow(fun a k sub -> 
                let (Flow f) = computation
                try
                    f a (fun vt -> 
                        match vt with
                        | Value v -> try let (Flow f') = bindFunc v in f' a k sub with _ as ex -> k <| Exception ex
                        | Exception ex -> k <| Exception ex
                    ) sub
                with _ as ex -> k <| Exception ex
            )
    
        member flow.Return(v: 'T): Flow<'T> =
            Flow(fun _ k _ -> k <| Value v)
    
        member flow.ReturnFrom(computation: Flow<'T>): Flow<'T> = computation
    
        member flow.Zero() = Flow(fun _ k _ -> k <| Value ())
    
        member flow.Delay(f: unit -> Flow<'T>): Flow<'T> = f()
    
        member flow.Combine(first: Flow<unit>, second: Flow<'T>): Flow<'T> =
            Flow(fun a k sub -> 
                let (Flow f) = first
                try
                    f a (fun vu ->
                        match vu with
                        | Value () -> let (Flow f') = second in try f' a k sub with _ as ex -> k <| Exception ex
                        | Exception ex -> k <| Exception ex
                    ) sub
                with _ as ex -> k <| Exception ex
            )
    
        member flow.TryWith(tried: Flow<'T>, withed: exn -> Flow<'T>): Flow<'T> =
            Flow(fun a k sub ->
                let (Flow f) = tried
                try 
                    f a (fun r -> 
                        match r with 
                        | Value v -> k <| Value v 
                        | Exception ex ->
                            try
                                let (Flow f') = withed ex in f' a k sub
                            with _ as ex ->
                                k <| Exception ex
                    ) sub
                with _ as ex -> 
                    try
                        let (Flow f') = withed ex
                        f' a k sub
                    with _ as ex -> k <| Exception ex
            )
    
        member flow.TryFinally(tried: Flow<'T>, finallied: unit -> unit): Flow<'T> =
            Flow(fun a k sub ->
                let (Flow f) = tried
                try
                    f a (fun v -> k v; finallied()) sub
                with _ as ex -> 
                    k <| Exception ex
                    finallied()
            )
