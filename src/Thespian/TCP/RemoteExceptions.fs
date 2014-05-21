namespace Nessos.Thespian.Remote

    open System
    open System.Net
    open System.Runtime.Serialization

    open Nessos.Thespian
    open Nessos.Thespian.ConcurrencyTools

    [<AutoOpen>]
    module Utils = 
#if PROTOCOLTRACE
        let sprintfn fmt = 
            Printf.ksprintf Console.WriteLine fmt
#else
        let sprintfn fmt = Printf.ksprintf ignore fmt
#endif

        let raisex (e: #exn): Async<'T> = Async.FromContinuations(fun (_,econt,_) -> econt e)

        let sleepx (timeout: int) = async {
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

        let withTimeout (timeout: int) (comp: Async<'T>): Async<'T> = async {
            let! ct = Async.CancellationToken
            return! Async.FromContinuations(fun (success, error, cancel) ->
                let ctsTimeout = System.Threading.CancellationTokenSource.CreateLinkedTokenSource([| ct |])
                let ctsComp = System.Threading.CancellationTokenSource.CreateLinkedTokenSource([| ct |])
                let k = async {
                    let! r = Async.Catch comp
                    ctsTimeout.Cancel()
                    match r with
                    | Choice1Of2 v -> success v
                    | Choice2Of2 e -> error e
                }
                let t = async {
                    do! sleepx timeout
                    //sprintfn "TIMEOUT %A" c
                    ctsComp.Cancel()
                    error (new System.TimeoutException("Workflow timeout."))
                }
                Async.Start(k, ctsComp.Token)
                Async.Start(t, ctsTimeout.Token)
            )
        }

        /// higher-order thread-safe memoizer operator
        let memoizeAsync (f: 'U -> Async<'T>) =
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

    [<Serializable>]
    type TcpProtocolConfigurationException =
        inherit Exception

        new(message: string) = {
            inherit Exception(message)
        }

        new(message: string, innerException: Exception) = {
            inherit Exception(message, innerException)
        }

        public new(info: SerializationInfo, context: StreamingContext) =
            { inherit Exception(info, context) }

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) = 
                base.GetObjectData(info, context)

    type SocketListenerException = 
        inherit CommunicationException

        val private endPoint: IPEndPoint

        new(message: string, ipEndPoint: IPEndPoint) = {
            inherit CommunicationException(message)

            endPoint = ipEndPoint
        }

        new(message: string,ipEndPoint: IPEndPoint, innerException: Exception) = {
            inherit CommunicationException(message, innerException)

            endPoint = ipEndPoint
        }

        internal new(info: SerializationInfo, context: StreamingContext) = { 
            inherit CommunicationException(info, context) 

            endPoint = info.GetValue("endPoint", typeof<IPEndPoint>) :?> IPEndPoint
        }

        member e.EndPoint = e.endPoint

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) =
                info.AddValue("endPoint", e.endPoint)



