namespace Nessos.Thespian.Utilities

    open System
    open System.IO
    open System.Threading
    open System.Threading.Tasks
    open Microsoft.FSharp.Control

    [<AutoOpen>]
    module AsyncExtensions =

        /// Generic exception container used for Async.Choice implementation
        type private SuccessException<'T> (value : 'T) = 
            inherit Exception()
            member __.Value = value


        // Task monadic bind extensions
        type AsyncBuilder with
            member __.Bind(f : Task<'T>, g : 'T -> Async<'S>) = __.Bind(Async.AwaitTask f, g)
            member __.Bind(f : Task, g : unit -> Async<'S>) = __.Bind(f.ContinueWith ignore |> Async.AwaitTask, g)


        type TaskCompletionSource<'T> with
            /// <summary>
            ///     Asynchronously await result or give up on timeout
            /// </summary>
            /// <param name="timeout">timeout in milliseconds.</param>
            member self.AsyncWaitResult(timeout : int): Async<'T option> = 
                Async.AwaitTask <| self.Task.TimeoutAfter timeout

            /// <summary>
            ///     Asynchronously await result
            /// </summary>
            member self.AsyncWaitResult() : Async<'T> = Async.AwaitTask self.Task


        type Async with

            /// <summary>
            ///     Continuation-based raise operator.
            /// </summary>
            /// <param name="e">exception to be raised.</param>
            static member Raise (e : #exn) =
#if DEBUG
                // when in debug mode, use CLR to raise exception
                async { return raise e }
#else
                Async.FromContinuations(fun (_,econt,_) -> econt e)
#endif

            /// <summary>
            /// 
            /// </summary>
            /// <param name="condition"></param>
            /// <param name="computation"></param>
            static member ConditionalCancel (condition: Async<bool>) (computation: Async<'T>): Async<'T option> = async {
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

            /// <summary>
            /// 
            /// </summary>
            /// <param name="timeout"></param>
            /// <param name="computation"></param>
            static member WithTimeout (timeout: int) (computation: Async<'T>): Async<'T option> =
                if timeout = Timeout.Infinite then async { let! r = computation in return Some r }
                elif timeout = 0 then async.Return None
                else async { let t = Async.StartAsTask computation in return! Async.AwaitTask <| t.TimeoutAfter(timeout) }

            static member AwaitTask(task: Task<'T>, timeout: int) =
                if timeout = Timeout.Infinite then async { let! r = Async.AwaitTask task in return Some r }
                elif timeout = 0 then async.Return None
                else Async.AwaitTask <| task.TimeoutAfter timeout

            /// <summary>
            ///     a version of Async.FromBeginEnd with a timeout parameter
            ///     the async workflow returns a Some value when the operation completes in time
            ///     otherwise returns a None value
            ///     On timeout, the async operation needs to be cancelled. This depends on the
            ///     particular resource the async operation is acting on. In the case of a Socket
            ///     the socket instance would have to be disposed.
            /// </summary>
            /// <param name="beginF"></param>
            /// <param name="endF"></param>
            /// <param name="timeout"></param>
            /// <param name="timeoutDisposeF"></param>
            static member TryFromBeginEnd(beginF: AsyncCallback * obj -> IAsyncResult, endF: IAsyncResult -> 'T, timeout: int, timeoutDisposeF: unit -> unit): Async<'T option> =
                if timeout = 0 then async.Return None
                else
                    //Use a Timer to dispose the stream on the timeout
                    //at which point EndRead raises an ObjectDisposed exception
                    //if the operation completed immediately then timer is not set
                    let timer = ref Unchecked.defaultof<Timer>
                    //Use the latch to determine whether timeout has occurred in the async callback
                    let latch = ref 0
                    Async.FromBeginEnd((fun (callback, state) ->
                        let iar: IAsyncResult = beginF(callback, state)
                        if not iar.IsCompleted && not (timeout = Timeout.Infinite) then
                            //only create the timer if the operation has not already completed
                            timer := new Timer(
                                //set the latch in the timer callback
                                //latch may have been set by the async callback, meaning operation has just completed,
                                //and so, nothing is disposed
                                (fun _ -> if Interlocked.CompareExchange(latch, 1, 0) = 0 then timer.Value.Dispose(); timeoutDisposeF()),
                                null, timeout, Timeout.Infinite)
                        iar),
                        (fun iar ->
                            //timer no longer needed
                            if timer.Value <> Unchecked.defaultof<Timer> then timer.Value.Dispose()
                            //if the current thread sets the latch, then no timeout will occur
                            //if latch has been already set, timeout occurs,
                            //that is: the resource is a) already disposed or, b) will be disposed
                            //if (a) then endF raises the exception, otherwise
                            //if (b) there is a result, but the result must be discarded
                            //since the stream will be disposed shortly
                            if Interlocked.CompareExchange(latch, 1, 0) = 0 then let r = endF(iar) in Some r
                            else try let _ = endF(iar) in None with :? ObjectDisposedException -> None))

            /// <summary>
            /// 
            /// </summary>
            /// <param name="arg"></param>
            /// <param name="beginF"></param>
            /// <param name="endF"></param>
            /// <param name="timeout"></param>
            /// <param name="timeoutDisposeF"></param>
            static member TryFromBeginEnd(arg: 'U, beginF: 'U * AsyncCallback * obj -> IAsyncResult, endF: IAsyncResult -> 'T, timeout: int, timeoutDisposeF: unit -> unit): Async<'T option> =
                Async.TryFromBeginEnd((fun (callback, state) -> beginF(arg, callback, state)), endF, timeout, timeoutDisposeF)

            /// <summary>
            /// 
            /// </summary>
            /// <param name="arg1"></param>
            /// <param name="arg2"></param>
            /// <param name="beginF"></param>
            /// <param name="endF"></param>
            /// <param name="timeout"></param>
            /// <param name="timeoutDisposeF"></param>
            static member TryFromBeginEnd(arg1: 'U1, arg2: 'U2, beginF: 'U1 * 'U2 * AsyncCallback * obj -> IAsyncResult, endF: IAsyncResult -> 'T, timeout: int, timeoutDisposeF: unit -> unit): Async<'T option> =
                Async.TryFromBeginEnd((fun (callback, state) -> beginF(arg1, arg2, callback, state)), endF, timeout, timeoutDisposeF)

            /// <summary>
            /// 
            /// </summary>
            /// <param name="arg1"></param>
            /// <param name="arg2"></param>
            /// <param name="arg3"></param>
            /// <param name="beginF"></param>
            /// <param name="endF"></param>
            /// <param name="timeout"></param>
            /// <param name="timeoutDisposeF"></param>
            static member TryFromBeginEnd(arg1: 'U1, arg2: 'U2, arg3: 'U3, beginF: 'U1 * 'U2 * 'U3 * AsyncCallback * obj -> IAsyncResult, endF: IAsyncResult -> 'T, timeout: int, timeoutDisposeF: unit -> unit): Async<'T option> =
              Async.TryFromBeginEnd((fun (callback, state) -> beginF(arg1, arg2, arg3, callback, state)), endF, timeout, timeoutDisposeF)

            /// <summary>
            ///     Isolates triggering of cancellation in given asynchronous workflow.
            ///     Cancellation bubbles up through the parent as cancellation exception.
            /// </summary>
            /// <param name="computationF">Computation to be executed.</param>
            /// <param name="cancellationToken">Cancellation token to be used in child. Defaults to parent cancellation token.</param>
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

            /// <summary>
            ///     Correct sleep implementation.
            /// </summary>
            /// <param name="timeout">Timeout in milliseconds</param>
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

            /// <summary>
            ///     NonDeterministic Choice combinator; executed computations in parallel until 
            ///     one completes with 'Some' result, otherwise returning 'None' if all have completed
            ///     with 'None'.
            /// </summary>
            /// <param name="tasks">Tasks to be executed.</param>
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

            static member AwaitObservable(observable: IObservable<'T>, ?timeout) =
                let tcs = new TaskCompletionSource<'T>()
                let rec observer = (fun result ->
                    tcs.SetResult result
                    remover.Dispose())
                and remover: IDisposable = observable.Subscribe tcs.SetResult

                match timeout with
                | None -> tcs.AsyncWaitResult()
                | Some t ->
                    async {
                        let! r = tcs.AsyncWaitResult t

                        match r with
                        | None -> return! Async.Raise <| TimeoutException()
                        | Some v -> return v
                    }


        type Stream with
            member self.AsyncWriteBytes (bytes: byte []) =
                async {
                    do! self.AsyncWrite(BitConverter.GetBytes bytes.Length, 0, 4)
                    do! self.AsyncWrite(bytes, 0, bytes.Length)
                    do! self.FlushAsync()
                }

            member self.AsyncReadBytes(length: int) =
                let rec readSegment buf offset remaining =
                    async {
                        let! read = self.AsyncRead(buf, offset, remaining)
                        if read < remaining then return! readSegment buf (offset + read) (remaining - read)
                        else return ()
                    }

                async {
                    let bytes = Array.zeroCreate<byte> length
                    do! readSegment bytes 0 length
                    return bytes
                }

            member self.AsyncReadBytes() =
                async {
                    let! lengthArr = self.AsyncReadBytes 4
                    let length = BitConverter.ToInt32(lengthArr, 0)
                    return! self.AsyncReadBytes length
                }


        [<RequireQualifiedAccess>]
        module Async =

            /// <summary>
            ///     Async memoization combinator
            /// </summary>
            /// <param name="f">Async workflow to be memoized.</param>
            let memoize (f: 'U -> Async<'T>) =
                let cache = Atom Map.empty

                fun (id: 'U) -> 
                    async {
                        match cache.Value.TryFind id with
                        | None ->
                            let! result = f id
                            Atom.swap cache (fun c -> c.Add(id, result))
                            return result
                        | Some result -> return result
                    }


        [<RequireQualifiedAccess>]
        module List =

            /// <summary>
            ///     Async foldM operation on lists.
            /// </summary>
            /// <param name="foldF">Folding function.</param>
            /// <param name="state">Initial state.</param>
            /// <param name="items">Input list.</param>
            let rec foldAsync (foldF: 'U -> 'T -> Async<'U>) (state: 'U) (items: 'T list): Async<'U> =
                async {
                    match items with
                    | [] -> return state
                    | item::rest ->
                        let! nextState = foldF state item
                        return! foldAsync foldF nextState rest
                }

            /// <summary>
            ///     Conditional async foldM operations on lists.
            /// </summary>
            /// <param name="foldF"></param>
            /// <param name="state"></param>
            /// <param name="items"></param>
            let rec foldWhileAsync (foldF: 'U -> 'T -> Async<'U * bool>) (state: 'U) (items: 'T list): Async<'U> =
                async {
                    match items with
                    | [] -> return state
                    | item::rest ->
                        let! nextState, proceed = foldF state item
                        if proceed then return! foldWhileAsync foldF nextState rest 
                        else 
                            return nextState
                }

            /// <summary>
            ///     Async foldBack operation on lists.
            /// </summary>
            /// <param name="foldF">folding function.</param>
            /// <param name="items">input list.</param>
            /// <param name="state">initial state.</param>
            let foldBackAsync (foldF: 'T -> 'U -> Async<'U>) (items: 'T list) (state: 'U): Async<'U> =
                let rec loop is k = async {
                    match is with
                    | [] -> return! k state
                    | h::t -> return! loop t (fun acc -> async { let! acc' = foldF h acc in return! k acc' })
                }

                loop items async.Return

            /// <summary>
            ///     Async map function.
            /// </summary>
            /// <param name="mapF">Map function.</param>
            /// <param name="items">Input list.</param>
            let mapAsync (mapF: 'T -> Async<'U>) (items: 'T list): Async<'U list> =
                foldBackAsync (fun i is -> async { let! i' = mapF i in return i'::is }) items []

            /// <summary>
            ///     Async choose function.
            /// </summary>
            /// <param name="choiceF">choice function.</param>
            /// <param name="items">Input list.</param>
            let chooseAsync (choiceF: 'T -> Async<'U option>) (items: 'T list): Async<'U list> =
                foldBackAsync (fun i is -> async { let! r = choiceF i in return match r with Some i' -> i'::is | _ -> is }) items []


        [<RequireQualifiedAccess>]   
        module Array =

            /// <summary>
            ///     Async foldM operation on arrays.
            /// </summary>
            /// <param name="foldF">Folding function.</param>
            /// <param name="state">Initial state.</param>
            /// <param name="items">Input array.</param>
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
