namespace Nessos.Thespian

    module AsyncExtensions = 
    
        open System
        open System.Threading
        open System.Threading.Tasks

        open Nessos.Thespian.TaskExtensions

        exception ResultException of obj

        type internal Latch() =
            let mutable switch = 0
            member __.Trigger() = Interlocked.CompareExchange(&switch, 1, 0) = 0

        type private SuccessException<'T> (value : 'T) = 
            inherit Exception()
            member __.Value = value

        type Microsoft.FSharp.Control.Async with
            static member Raise (e : #exn) = async { return raise e } //Async.FromContinuations(fun (_,econt,_) -> econt e)

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


//            static member WithTimeout (timeout: int) (computation: Async<'T>): Async<'T option> = async {
//                let! ct = Async.CancellationToken
//                return! Async.FromContinuations(fun (success, error, cancel) ->
//                    let ctsTimeout = System.Threading.CancellationTokenSource.CreateLinkedTokenSource([| ct |])
//                    let ctsComp = System.Threading.CancellationTokenSource.CreateLinkedTokenSource([| ct |])
//                    let k = async {
//                        let! r = Async.Catch computation
//                        ctsTimeout.Cancel()
//                        match r with
//                        | Choice1Of2 v -> success (Some v)
//                        | Choice2Of2 e -> error e
//                    }
//                    let t = async {
//                        do! Async.Sleep timeout
//                        //sprintfn "TIMEOUT %A" c
//                        ctsComp.Cancel()
//                        //error (new System.TimeoutException("Workflow timeout."))
//                        success None
//                    }
//                    Async.Start(k, ctsComp.Token)
//                    Async.Start(t, ctsTimeout.Token)
//                )
//            }

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

            static member WithTimeout (timeout: int) (computation: Async<'T>): Async<'T option> =
              if timeout = Timeout.Infinite then async { let! r = computation in return Some r }
              elif timeout = 0 then async.Return None
              else async { let t = Async.StartAsTask computation in return! Async.AwaitTask <| t.TimeoutAfter(timeout) }

            // static member WithTimeout (timeout: int) (computation: Async<'T>): Async<'T option> =
            //     let cancelCondition = async {
            //         do! Async.Sleep timeout
            //         return true
            //     }
            //     Async.ConditionalCancel cancelCondition computation

//                let timeout = async {
//                    do! Async.Sleepx timeout
//                    return! Async.Raise (new TimeoutException("The operation has timed out."))
//                }
//                let operation = async {
//                    let! result = computation
//                    return! Async.Raise <| ResultException (box result)
//                }
//                let! result = Async.Parallel [operation; timeout] |> Async.Catch
//                match result with
//                | Choice2Of2(:? TimeoutException as e) -> return None
//                | Choice2Of2(ResultException v) -> return Some (unbox v)
//                | _ -> return failwith "Control flow failure."
//            }

            // untyped awaitTask
            // static member AwaitTask (t : Task) = t.ContinueWith ignore |> Async.AwaitTask
            // // non-blocking awaitTask with timeout
            // static member AwaitTask (t : Task<'T>, timeout : int) =
            //     async {
            //         let! ct = Async.CancellationToken
            //         use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
            //         use timer = Task.Delay (timeout, cts.Token)
            //         let tcs = new TaskCompletionSource<bool>()
            //         use _ = ct.Register(new Action<obj>(fun s -> (s :?> TaskCompletionSource<bool>).TrySetResult(true) |> ignore), tcs)
            //         try
            //             let! completed = Async.AwaitTask <| Task.WhenAny(t, tcs.Task, timer)
            //             if completed = (t :> Task) then
            //                 let! result = Async.AwaitTask t
            //                 return Some result
            //             else if completed = (tcs.Task :> Task) then
            //                 return raise (new OperationCanceledException(ct))
            //             else return None

            //         finally cts.Cancel()
            //     }

            static member AwaitTask(task: Task<'T>, timeout: int) =
              if timeout = Timeout.Infinite then async { let! r = Async.AwaitTask task in return Some r }
              elif timeout = 0 then async.Return None
              else Async.AwaitTask <| task.TimeoutAfter timeout
              
            //a version of Async.FromBeginEnd with a timeout parameter
            //the async workflow returns a Some value when the operation completes in time
            //otherwise returns a None value
            //On timeout, the async operation needs to be cancelled. This depends on the
            //particular resource the async operation is acting on. In the case of a Socket
            //the socket instance would have to be disposed.
            static member TryFromBeginEnd(beginF: AsyncCallback * obj -> IAsyncResult, endF: IAsyncResult -> 'T, timeout: int, timeoutDisposeF: unit -> unit): Async<'T option> =
              if timeout = 0 then async.Return None
              else
                //Use a Timer to dispose the stream on the timeout
                //at which point EndRead raises an ObjectDisposed exception
                //if the operation completed immediately then timer is not set
                let timer = ref Unchecked.defaultof<Timer>
                //Use the latch to determin whether timeout has occured in the async callback
                let latch = ref 0
                Async.FromBeginEnd(
                  (fun (callback, state) ->
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
                     timer.Value.Dispose()
                     //if the current thread sets the latch, then no timeout will occur
                     //if latch has been already set, timeout occurs,
                     //that is: the resource is a) already disposed or, b) will be disposed
                     //if (a) then endF raises the exception, otherwise
                     //if (b) there is a result, but the result must be discarded
                     //since the stream will be disposed shortly
                     if Interlocked.CompareExchange(latch, 1, 0) = 0 then let r = endF(iar) in Some r
                     else try let _ = endF(iar) in None with :? ObjectDisposedException -> None))

            static member TryFromBeginEnd(arg: 'U, beginF: 'U * AsyncCallback * obj -> IAsyncResult, endF: IAsyncResult -> 'T, timeout: int, timeoutDisposeF: unit -> unit): Async<'T option> =
              Async.TryFromBeginEnd((fun (callback, state) -> beginF(arg, callback, state)), endF, timeout, timeoutDisposeF)
            static member TryFromBeginEnd(arg1: 'U1, arg2: 'U2, beginF: 'U1 * 'U2 * AsyncCallback * obj -> IAsyncResult, endF: IAsyncResult -> 'T, timeout: int, timeoutDisposeF: unit -> unit): Async<'T option> =
              Async.TryFromBeginEnd((fun (callback, state) -> beginF(arg1, arg2, callback, state)), endF, timeout, timeoutDisposeF)
            static member TryFromBeginEnd(arg1: 'U1, arg2: 'U2, arg3: 'U3, beginF: 'U1 * 'U2 * 'U3 * AsyncCallback * obj -> IAsyncResult, endF: IAsyncResult -> 'T, timeout: int, timeoutDisposeF: unit -> unit): Async<'T option> =
              Async.TryFromBeginEnd((fun (callback, state) -> beginF(arg1, arg2, arg3, callback, state)), endF, timeout, timeoutDisposeF)

            static member AwaitObservableCorrect(source: IObservable<'T>) =
                let value : 'T option ref = ref None
                let subscription : IDisposable option ref = ref None
                let continuation : ('T -> unit) option ref = ref None

                let observer result =
                    lock source (fun _ ->
                        match subscription.Value with
                        | Some d -> 
                            d.Dispose()
                            subscription := None
                        | None -> ()
                        match continuation.Value with
                        | Some cont -> 
                            continuation := None
                            cont result
                        | None -> value := Some result
                    )

                subscription := Some (source.Subscribe observer)

                Async.FromContinuations((fun (cont, _, _) -> 
                    lock source (fun _ ->
                        match value.Value with
                        | Some result -> 
                            value := None

                            match subscription.Value with
                            | Some d -> 
                                d.Dispose()
                                subscription := None
                            | None -> ()

                            cont result
                        | None -> continuation := Some cont
                    )
                ))

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

            //correct sleep implementation
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

        open Nessos.Thespian.TaskExtensions
        
        type AsyncResultCell<'T>() =
            let completionSource = new TaskCompletionSource<'T>()
            member c.RegisterResult(result: 'T) = completionSource.SetResult(result)
            member c.AsyncWaitResult(millisecondsTimeout: int): Async<'T option> = Async.AwaitTask <| completionSource.Task.TimeoutAfter(millisecondsTimeout)
            member c.AsyncWaitResult(): Async<'T> = Async.AwaitTask(completionSource.Task)


        type AsyncResultCell2<'T>() =
            let spinLock = new SpinLock()
            let continuation: ('T -> unit) option ref = ref None
            let result: 'T option ref = ref None

            member cell.RegisterResult(resultValue: 'T) =
                let mutable gotLock = false
                spinLock.Enter(&gotLock)
                match result.Value with
                | Some _ ->
                    if gotLock then spinLock.Exit()
                    invalidOp "Cell value already registered."
                | None ->
                    match continuation.Value with
                    | Some success -> 
                        if gotLock then spinLock.Exit()
                        success resultValue
                    | None -> 
                        result := Some resultValue
                        if gotLock then spinLock.Exit()

            member cell.AsyncWaitResult(): Async<'T> = async {
                let! ct = Async.CancellationToken
                return! 
                    Async.FromContinuations(fun (success, error, cancellation) ->
                        if ct.IsCancellationRequested then cancellation (new OperationCanceledException())
                        else
                            let mutable gotLock = false
                            spinLock.Enter(&gotLock)
                            match continuation.Value with
                            | Some _ -> 
                                if gotLock then spinLock.Exit()
                                error (new InvalidOperationException("Another workflow is waiting for cell value."))
                            | None ->
                                match result.Value with
                                | Some value -> 
                                    if gotLock then spinLock.Exit()
                                    success value
                                | None -> 
                                    continuation := Some success
                                    if gotLock then spinLock.Exit()
                    )
            }

            member cell.AsyncWaitResult(millisecondsTimeout: int): Async<'T option> =
                cell.AsyncWaitResult() |> Async.WithTimeout millisecondsTimeout
                    


//             async {
//                let! result = c.AsyncWaitResult(Timeout.Infinite)
//                return match result with
//                        | Some r -> r
//                        | None -> raise <| new TimeoutException("Waiting for result in cell has timed out.")
//            }

//        type AsyncResultCell<'T>() =
//            let event = new Event<'T>()
//
//            member c.OnSetResult = event.Publish
//            member c.RegisterResult(result: 'T) = event.Trigger result
//            member c.AsyncWaitResult(millisecondsTimeout: int): Async<'T option> =
//                async {
//                    let! result = Async.AwaitObservableCorrect c.OnSetResult
//
//                    return Some result
//                }
//            member c.AsyncWaitResult(): Async<'T> =
//                Async.AwaitObservableCorrect c.OnSetResult

        type Microsoft.FSharp.Control.Async with 
            static member AwaitObservable(observable: IObservable<'T>, ?timeout) =
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

        // Implementation of the 'AwaitEvent' primitive
        // From Real World Functional Programming
        // NOTE!!!! THIS IMLEMENTATION IS HIGHLY PROBLEMATIC
        // The actual registrastion on the observable does not
        // occur until the control flow actually reaches
        // a monadic bind.
        // The problem is that the event we want to observe could
        // have been triggered before the monadic bind.
        // A correct implementation would be to register the callback
        // on the observable on construction of the Async
        type Microsoft.FSharp.Control.Async with 
            static member AwaitObservableWrong(ev1:IObservable<'a>) = 
                Async.FromContinuations((fun (cont,econt,ccont) -> 
                    let rec callback = (fun value ->
                        remover.Dispose()
                        cont(value) )
                    and remover : IDisposable  = ev1.Subscribe(callback) 
                    () ))

            static member AwaitObservableWrong(ev1:IObservable<'a>, ev2:IObservable<'b>) = 
                Async.FromContinuations((fun (cont,econt,ccont) ->
                    let rec callback1 = (fun value ->
                        remover1.Dispose()
                        remover2.Dispose()
                        cont(Choice1Of2(value)) )

                    and callback2 = (fun value ->
                        remover1.Dispose()
                        remover2.Dispose()
                        cont(Choice2Of2(value)) )

                    and remover1 : IDisposable  = ev1.Subscribe(callback1) 
                    and remover2 : IDisposable  = ev2.Subscribe(callback2) 
                    () ))

            static member AwaitObservableWrong(ev1:IObservable<'a>, ev2:IObservable<'b>, ev3:IObservable<'c>) = 
                Async.FromContinuations((fun (cont,econt,ccont) -> 
                    let rec callback1 = (fun value ->
                        remover1.Dispose()
                        remover2.Dispose()
                        remover3.Dispose()
                        cont(Choice1Of3(value)) )

                    and callback2 = (fun value ->
                        remover1.Dispose()
                        remover2.Dispose()
                        remover3.Dispose()
                        cont(Choice2Of3(value)) )

                    and callback3 = (fun value ->
                        remover1.Dispose()
                        remover2.Dispose()
                        remover3.Dispose()
                        cont(Choice3Of3(value)) )

                    and remover1 : IDisposable  = ev1.Subscribe(callback1) 
                    and remover2 : IDisposable  = ev2.Subscribe(callback2) 
                    and remover3 : IDisposable  = ev3.Subscribe(callback3) 
                    () ))


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

        let rec conditionalFoldAsync (foldF: 'U -> 'T -> Async<'U * bool>) (state: 'U) (items: 'T list): Async<'U> =
          async {
            match items with
            | [] -> return state
            | item::rest ->
              let! nextState, proceed = foldF state item
              if proceed then return! conditionalFoldAsync foldF nextState rest else return nextState
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
