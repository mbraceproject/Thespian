namespace Nessos.Thespian.Tools

    [<AutoOpen>]
    module TaskExtensions =

        open System
        open System.Threading
        open System.Threading.Tasks

        type System.Threading.Tasks.Task<'T> with
            member t.TimeoutAfter(timeout : int) : Task<'T option> = 
                if t.IsCompleted || (timeout = Timeout.Infinite) then 
                    t.ContinueWith
                        (new Func<Task<'T>, 'T option>(fun antecendant -> Some antecendant.Result), 
                        TaskContinuationOptions.NotOnCanceled 
                        ||| TaskContinuationOptions.NotOnFaulted 
                        ||| TaskContinuationOptions.ExecuteSynchronously)
                else 
                    let tcs = new TaskCompletionSource<'T option>()
                    if timeout = 0 then 
                        tcs.TrySetResult(None) |> ignore
                        tcs.Task
                    else 
                        let timer = 
                            new Timer(new TimerCallback(fun state -> 
                                      let tcs' = state :?> TaskCompletionSource<'T option>
                                      tcs'.TrySetResult(None) |> ignore), tcs, timeout, Timeout.Infinite)
                        t.ContinueWith
                            (new Action<Task<'T>, obj>(fun antecendant state -> 
                                let (timer', tcs'') = state :?> Timer * TaskCompletionSource<'T option>
                                timer'.Dispose()
                                match antecendant.Status with
                                | TaskStatus.Faulted -> tcs''.TrySetException(antecendant.Exception) |> ignore
                                | TaskStatus.Canceled -> tcs''.TrySetCanceled() |> ignore
                                | TaskStatus.RanToCompletion -> tcs''.TrySetResult(Some antecendant.Result) |> ignore
                                | _ -> failwith "unexpected task state"), 
                            (timer, tcs), 
                            CancellationToken.None, 
                            TaskContinuationOptions.ExecuteSynchronously, 
                            TaskScheduler.Default)
                        |> ignore
                        tcs.Task