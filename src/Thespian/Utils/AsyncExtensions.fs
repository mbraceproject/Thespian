namespace Nessos.Thespian.ConcurrencyTools

    open System
    open System.Threading
    open System.Threading.Tasks

    [<AutoOpen>]
    module AsyncExtensions = 

        // used for the Async.Choice implementation

        type private SuccessException<'T> (value : 'T) = 
            inherit Exception()
            member __.Value = value

        //Based on
        //http://moiraesoftware.com/blog/2012/01/30/FSharp-Dataflow-agents-II/
        //but it is simplified
        type AsyncResultCell<'T>() =
            let completionSource = new TaskCompletionSource<'T>()

            let t = completionSource.Task

            member c.RegisterResult<'T>(result: 'T) = completionSource.SetResult(result)
            member c.AsyncWaitResult<'T>(millisecondsTimeout: int): Async<'T option> =
                Async.AwaitTask<'T>(completionSource.Task, millisecondsTimeout)

            // use default AwaitTask when no timeout overload is given
            member c.AsyncWaitResult<'T>(): Async<'T> = async {
                let! r = Async.AwaitTask<'T>(completionSource.Task, Timeout.Infinite)
                return Option.get r
            }

        and Microsoft.FSharp.Control.Async with

            /// Performant Async.Raise
            static member Raise (e : #exn) =
// when debug, need to notify the CLR that an exception is raised
#if DEBUG
                async { return raise e }
#else
                Async.FromContinuations(fun (_,econt,_) -> econt e)
#endif

            static member Sleepx (timeout: int) = async {
                let! ct = Async.CancellationToken
                let tmr = ref (null : System.Threading.Timer)
                let cancellationCont = ref (ignore : System.OperationCanceledException -> unit)
                use! cancelHandler = Async.OnCancel(fun () -> (if tmr.Value <> null then tmr.Value.Dispose()); cancellationCont.Value (new System.OperationCanceledException()))
                do! Async.FromContinuations(fun (success, error, cancel) ->
                    cancellationCont := cancel
                    tmr := 
                        new System.Threading.Timer(
                            new System.Threading.TimerCallback(fun _ -> if not ct.IsCancellationRequested then success()), 
                            null, 
                            timeout, 
                            System.Threading.Timeout.Infinite
                        )
                )
            }


            static member ConditionalCancel<'T> (condition: Async<bool>) (computation: Async<'T>): Async<'T option> = async {
                let! ct = Async.CancellationToken
                return! Async.FromContinuations(fun (success, error, _) ->
                    let ctsTimeout = System.Threading.CancellationTokenSource.CreateLinkedTokenSource([| ct |])
                    let ctsComp = System.Threading.CancellationTokenSource.CreateLinkedTokenSource([| ct |])
                    let latch = new Latch()
                    let pSuccess v = if latch.Trigger() then success v
                    let pError e = if latch.Trigger() then error e
                    let k = async {
                        let! r = Async.Catch computation
                        ctsTimeout.Cancel()
                        match r with
                        | Choice1Of2 v -> pSuccess (Some v)
                        | Choice2Of2 e -> pError e
                    }
                    let t = async {
                        let! r = Async.Catch condition
                        match r with
                        | Choice1Of2 true -> 
                            ctsComp.Cancel()
                            pSuccess None
                        | Choice1Of2 false -> ()
                        | Choice2Of2 e ->
                            ctsComp.Cancel()
                            pError e
                    }
                    Async.Start(k, ctsComp.Token)
                    Async.Start(t, ctsTimeout.Token)
                )
            }

            static member WithTimeout<'T> (timeout: int) (computation: Async<'T>): Async<'T option> =
                let cancelCondition = async {
                    do! Async.Sleep timeout
                    return true
                }

                Async.ConditionalCancel cancelCondition computation

            /// untyped awaitTask
            static member AwaitTask (t : Task) = t.ContinueWith ignore |> Async.AwaitTask

            /// non-blocking awaitTask with timeout
            static member AwaitTask<'T> (t : Task<'T>, timeout : int) : Async<'T option> =
                async {
                    let! ct = Async.CancellationToken
                    use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
                    use timer = Task.Delay (timeout, cts.Token)
                    let tcs = new TaskCompletionSource<bool>()
                    use _ = ct.Register(new Action<obj>(fun s -> (s :?> TaskCompletionSource<bool>).TrySetResult(true) |> ignore), tcs)
                    try
                        let! completed = Async.AwaitTask <| Task.WhenAny(t, tcs.Task, timer)
                        if completed = (t :> Task) then
                            let! result = Async.AwaitTask t
                            return Some result
                        else if completed = (tcs.Task :> Task) then
                            return raise (new OperationCanceledException(ct))
                        else return None

                    finally cts.Cancel()
                }

            static member IsolateCancellation (computationF : CancellationToken -> Async<'T>, ?cancellationToken : CancellationToken) : Async<'T> =
                async {
                    let! ct = 
                        match cancellationToken with
                        | None -> Async.CancellationToken
                        | Some ct -> async.Return ct

                    try
                        return! Async.AwaitTask <| Async.StartAsTask(computationF ct)
                    with :? AggregateException as e when e.InnerExceptions.Count = 1 ->
                        return! Async.Raise <| e.InnerExceptions.[0]
                }

            /// correct sleep implementation
            static member SleepSafe (timeout: int) = async {
                let! ct = Async.CancellationToken
                let tmr = ref (null : System.Threading.Timer)
                let cancellationCont = ref (ignore : System.OperationCanceledException -> unit)
                use! cancelHandler = Async.OnCancel(fun () -> (if tmr.Value <> null then tmr.Value.Dispose()); cancellationCont.Value (new System.OperationCanceledException()))
                do! Async.FromContinuations(fun (success, error, cancel) ->
                    cancellationCont := cancel
                    tmr := 
                        new System.Threading.Timer(
                            new System.Threading.TimerCallback(fun _ -> if not ct.IsCancellationRequested then success()), 
                            null, 
                            timeout, 
                            System.Threading.Timeout.Infinite
                        )
                )
            }

            /// nondeterministic choice
            static member Choice<'T>(tasks : Async<'T option> seq) : Async<'T option> =
                let wrap task =
                    async {
                        let! res = task
                        match res with
                        | None -> return ()
                        | Some r -> return! Async.Raise <| SuccessException r
                    }

                async {
                    try
                        do!
                            tasks
                            |> Seq.map wrap
                            |> Async.Parallel
                            |> Async.Ignore

                        return None
                    with 
                    | :? SuccessException<'T> as ex -> return Some ex.Value
                }

            static member AwaitObservable<'T>(observable: IObservable<'T>, ?timeout) =
                let resultCell = new AsyncResultCell<'T>()
                let rec observer = (fun result ->
                    resultCell.RegisterResult(result)
                    remover.Dispose())
                and remover: IDisposable = observable.Subscribe resultCell.RegisterResult

                match timeout with
                | None -> resultCell.AsyncWaitResult()
                | Some t ->
                    async {
                        let! r = resultCell.AsyncWaitResult t
                        
                        match r with
                        | None -> return! Async.Raise <| TimeoutException()
                        | Some v -> return v
                    }


    [<RequireQualifiedAccess>]
    module List =
        let rec foldAsync (foldF: 'U -> 'T -> Async<'U>) (state: 'U) (items: 'T list): Async<'U> =
            async {
                match items with
                | [] -> return state
                | item::rest ->
                    let! nextState = foldF state item
                    return! foldAsync foldF nextState rest
            }

        let foldBackAsync (foldF: 'T -> 'U -> Async<'U>) (items: 'T list) (state: 'U): Async<'U> =
            let rec loop is k = async {
                match is with
                | [] -> return! k state
                | h::t -> return! loop t (fun acc -> async { let! acc' = foldF h acc in return! k acc' })
            }

            loop items async.Return

        let mapAsync (mapF: 'T -> Async<'U>) (items: 'T list): Async<'U list> =
            foldBackAsync (fun i is -> async { let! i' = mapF i in return i'::is }) items []

        let chooseAsync (choiceF: 'T -> Async<'U option>) (items: 'T list): Async<'U list> =
            foldBackAsync (fun i is -> async { let! r = choiceF i in return match r with Some i' -> i'::is | _ -> is }) items []
         
    [<RequireQualifiedAccess>]   
    module Array =
        let foldAsync (foldF: 'U -> 'T -> Async<'U>) (state: 'U) (items: 'T []): Async<'U> =
            let rec foldArrayAsync foldF state' index =
                async {
                    if index = items.Length then
                        return state'
                    else
                        let! nextState = foldF state' items.[index]
                        return! foldArrayAsync foldF nextState (index + 1)
                }
            foldArrayAsync foldF state 0
    

    [<RequireQualifiedAccess>]
    module Async =
        /// postcompose covariant operation
        let map (f : 'T -> 'S) (w : Async<'T>) : Async<'S> =
            async { let! r = w in return f r }

        /// lifting of lambdas to async funcs
        let lift (f : 'T -> 'S) = fun t -> async { return f t }

        /// nodeterministic pick
        let tryPick (f : 'T -> Async<'S option>) (ts : seq<'T>) : Async<'S option> =
            ts |> Seq.map f |> Async.Choice

        /// nondeterministic pick
        let pick (f : 'T -> Async<'S>) (ts : seq<'T>) : Async<'S> =
            async {
                let! result = ts |> Seq.map (fun t -> map Some (f t)) |> Async.Choice

                return result.Value
            }

        /// nondeterministic forall
        let forall (f : 'T -> Async<bool>) (ts : seq<'T>) : Async<bool> =
            let wrapper t = map (function true -> None | false -> Some ()) (f t)
            ts |> Seq.map wrapper |> Async.Choice |> map Option.isNone

        /// nondeterministic existential
        let exists (f : 'T -> Async<bool>) (ts : seq<'T>) : Async<bool> =
            let wrapper t = map (function true -> Some () | false -> None) (f t)
            ts |> Seq.map wrapper |> Async.Choice |> map Option.isSome