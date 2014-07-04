module Nessos.Thespian.Remote.TcpProtocol.ConnectionPool

open System
open System.Threading
open System.Net
open System.Net.Sockets
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics
open Nessos.Thespian
open Nessos.Thespian.AsyncExtensions
open Nessos.Thespian.Remote.SocketExtensions


let inline private debug prefix x = ignore() //Debug.writelfc (sprintf "ConnectionPool::%s:: " prefix) x
let inline private connectionEndPoints (client: TcpClient) =
    if client.Client <> null then client.Client.LocalEndPoint, client.Client.RemoteEndPoint else null, null

type IClientConnectionPool =
    abstract AsyncAcquireConnection: string -> Async<PooledTcpClient>
    abstract ReleaseConnection: PooledTcpClient -> unit
    abstract GetCounters: unit -> int * int * int
    abstract Clear: unit -> unit

and private ConnectionPoolMsg =
    | AcquireConnection of IReplyChannel<PooledTcpClient> * string
    | GetCounters of IReplyChannel<int * int * int>
    | ReleaseConnection of PooledTcpClient

and SequentialClientConnectionPool(endPoint: IPEndPoint, minConnections: int, maxConnections: int, retryInterval: int) as self =
    let available = new Queue<TcpClient>()
    let pendingReplies = new Queue<Reply<PooledTcpClient> -> unit>()
    let mutable occupied = 0

    let isConnected (client: TcpClient) =
      try
        let socket = client.Client
        if socket = null then false
        elif socket.Connected = false then false
        elif socket.Poll(0, SelectMode.SelectRead) && socket.Available = 0 then false
        else true
      with _ -> false

    let setConnectionOptions (client: TcpClient) =
        //disable Naggle
        client.Client.NoDelay <- true
        //enable keep-alive
        //client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true)

    let connectSync tracePrefix (client: TcpClient) =
        //debug tracePrefix "CONNECTING"
        client.Connect endPoint
        setConnectionOptions client
        //debug tracePrefix "CONNECTED %A" (client.Client.LocalEndPoint, client.Client.RemoteEndPoint)

    //Throws
    //ObjectDisposedException => connection was closed
    //SocketException => something went wrong
    let connectAsync tracePrefix (client: TcpClient) = async {
        //debug tracePrefix "ASYNC-CONNECTING"
        do! client.AsyncConnent endPoint
        setConnectionOptions client

        //debug tracePrefix "ASYNC-CONNECTED %A" (client.Client.LocalEndPoint, client.Client.RemoteEndPoint)
    }

    let getAvailableAsync tracePrefix = async {
        let client = new TcpClient()
        do! connectAsync tracePrefix client
        return client
    }

    let getAvailableSync tracePrefix = 
        let client = new TcpClient()
        connectSync tracePrefix client
        client

    let addAvailableAsync tracePrefix = async {
        let! client = getAvailableAsync tracePrefix
        available.Enqueue client
    }

    let addAvailableSync tracePrefix =
        let client = getAvailableSync tracePrefix
        available.Enqueue client

    let poolClient tracePrefix client = new PooledTcpClient(tracePrefix, client, self :> IClientConnectionPool)

    let asyncAcquireClient (tracePrefix: string) (reply: Reply<PooledTcpClient> -> unit) = 
        async {
            if available.Count = 0 then 
                pendingReplies.Enqueue reply
            else
                let freeClient = available.Dequeue()
                if not (isConnected freeClient) then
                    //ensure disposal
                    freeClient.Close()
                    //get new client
                    let! newClientResult = getAvailableAsync() |> Async.Catch
                    match newClientResult with
                    | Choice1Of2 newClient ->
                        occupied <- occupied + 1
                        return reply <| Value(poolClient tracePrefix newClient)
                    | Choice2Of2 e -> 
                        available.Enqueue(new TcpClient()) 
                        return reply (Exception e)
                else
                    occupied <- occupied + 1
                    return reply <| Value(poolClient tracePrefix freeClient)
        }

    let releaseConnection(client: PooledTcpClient) =
      async {
        if pendingReplies.Count <> 0 then 
            let reply = pendingReplies.Dequeue()
            if not (isConnected client.UnderlyingClient) then
//                printfn "NOT CONNECTED"
                try
                    //ensure disposal
                    client.UnderlyingClient.Close()
                    let! client' = getAvailableAsync() 
                    reply <| Value(poolClient client.TracePrefix client')
                with e ->
                    occupied <- occupied - 1
                    available.Enqueue (new TcpClient())
                    reply (Exception e)
            else 
                reply <| Value(poolClient client.TracePrefix client.UnderlyingClient)
        else
            occupied <- occupied - 1
            available.Enqueue client.UnderlyingClient
      }

    let getCounters() =
      let availableConnections = available.Count
      let occupiedConnections = occupied
      let pendingRequests = pendingReplies.Count
      availableConnections, occupiedConnections, pendingRequests

    let processor = new Actor<_>(fun (actor: Actor<ConnectionPoolMsg>) -> 
        let rec messageLoop() = async {
            let! msg = actor.Receive()
            match msg with
            | AcquireConnection(R reply, tracePrefix) ->
                try
                    do! asyncAcquireClient tracePrefix reply
//                    printfn "Acquired: %A" <| getCounters()
                with e -> reply <| Exception e
                
                return! messageLoop()
            | GetCounters(R reply) ->
               reply <| Value (getCounters())
               return! messageLoop()
            | ReleaseConnection client ->
                do! releaseConnection client
//                printfn "Realeased: %A" <| getCounters()
                return! messageLoop()
        }
        messageLoop())

    do
        if minConnections > maxConnections then invalidArg "minConnections" "Minimum number of connections is greater than maximum."

        [1..minConnections] |> Seq.iter (fun _ -> addAvailableSync())
        [1..(maxConnections - minConnections)] |> Seq.iter (fun _ -> available.Enqueue(new TcpClient()))
        //debug String.Empty "Constructed ClientConnectionPool(minConnections = %d, maxConnections = %d, retryInterval = %d)" minConnections maxConnections retryInterval
        processor.Start()

    interface IClientConnectionPool with
        override self.AsyncAcquireConnection(tracePrefix: string): Async<PooledTcpClient> = async {
            try
               //TODO!!! make the timeout configurable
               return! !processor <!- fun rc -> AcquireConnection(rc.WithTimeout(Timeout.Infinite), tracePrefix)
            with MessageHandlingException(_, e) -> return! Async.Raise e
        }

        override self.ReleaseConnection(client: PooledTcpClient): unit = 
            !processor <-- ReleaseConnection client

        override self.GetCounters() = !processor <!= GetCounters

        override self.Clear(): unit =
            //debug String.Empty "CLEARING"
            for connection in available do connection.Close()


and PooledTcpClient(tracePrefix: string, tcpClient: TcpClient, clientPool: IClientConnectionPool) =
    let timeCreated = DateTime.Now
    [<VolatileField>]
    let mutable latch = 0
    let triggerLatch() = Interlocked.CompareExchange(&latch, 1, 0) = 0

    member internal __.TracePrefix = tracePrefix
    member internal __.UnderlyingClient = tcpClient
    
    member __.TimeCreated = timeCreated

    member self.Return() = 
        if triggerLatch() then clientPool.ReleaseConnection self

    member self.GetStream() = new PooledNetworkStream(self) :> ProtocolNetworkStream

    interface IDisposable with
        override self.Dispose() = self.Return()

and PooledNetworkStream(client: PooledTcpClient) =
    inherit ProtocolNetworkStream(client.UnderlyingClient, true)

    override __.Close() = base.Close(); client.Return()
    override self.FaultDispose() = self.Dispose();

    interface IDisposable with override __.Dispose() = base.Dispose(); client.Return()


type TcpConnectionPool() =
    static let DEFAULT_MIN_CONNECTIONS = 5
    static let DEFAULT_MAX_CONNECTIONS = 20
    static let DEFAULT_RETRY_INTERVAL = 500

    static let mutable MIN_CONNECTIONS = DEFAULT_MIN_CONNECTIONS
    static let mutable MAX_CONNECTIONS = DEFAULT_MAX_CONNECTIONS
    static let mutable RETRY_INTERVAL = DEFAULT_RETRY_INTERVAL

    static let pool = new ConcurrentDictionary<IPEndPoint, IClientConnectionPool>()

    static member Init(?minConections: int, ?maxConnections: int, ?retryInterval: int) =
        MIN_CONNECTIONS <- defaultArg minConections DEFAULT_MIN_CONNECTIONS
        MAX_CONNECTIONS <- defaultArg maxConnections DEFAULT_MAX_CONNECTIONS
        RETRY_INTERVAL <- defaultArg retryInterval DEFAULT_RETRY_INTERVAL
        //debug String.Empty "Initialized MIN_CONNECTIONS=%d, MAX_CONNECTIONS=%d, RETRY_INTERVAL=%d" MIN_CONNECTIONS MAX_CONNECTIONS RETRY_INTERVAL

    static member Fini() =
        debug String.Empty "Finalizing..."

        for kv in pool do
            kv.Value.Clear()

        pool.Clear()
        debug String.Empty "Finalized"
        

    static member AsyncAcquireConnection(tracePrefix: string, endPoint: IPEndPoint) = async {
        //debug tracePrefix "Acquiring connection for endPoint=%A" endPoint
        let clientPool = pool.GetOrAdd(endPoint, fun endPoint' -> new SequentialClientConnectionPool(endPoint, MIN_CONNECTIONS, MAX_CONNECTIONS, RETRY_INTERVAL) :> IClientConnectionPool)

        return! clientPool.AsyncAcquireConnection tracePrefix
    }

    static member AsyncAcquireConnection(endPoint: IPEndPoint) = TcpConnectionPool.AsyncAcquireConnection(String.Empty, endPoint)

    static member GetCounters() =
      pool |> Seq.map (fun kv -> kv.Key.ToString(), kv.Value.GetCounters()) |> Seq.toArray












// and ParallelClientConnectionPool(endPoint: IPEndPoint, minConnections: int, maxConnections: int, retryInterval: int) as self =
//     let latchAcquire = new CountdownLatch()
//     let latchRelease = new CountdownLatch()
//     let debug prefix x = debug (sprintf "%A::%A" endPoint prefix) x

//     let available = new ConcurrentQueue<TcpClient>()
//     let pendingRequests = new ConcurrentQueue<(TcpClient -> unit) * (exn -> unit) * (OperationCanceledException -> unit) * CancellationToken>()

//     let isConnected (client: TcpClient) =
//         let socket = client.Client
//         if socket = null then false
//         elif socket.Connected = false then false
//         elif socket.Poll(0, SelectMode.SelectRead) && socket.Available = 0 then false
//         else true

//     let setConnectionOptions (client: TcpClient) =
//         //disable Naggle
//         client.Client.NoDelay <- true
//         //enable keep-alive
//         //client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true)

//     let connectSync tracePrefix (client: TcpClient) =
//         //debug tracePrefix "CONNECTING"
//         client.Connect endPoint
//         setConnectionOptions client
//         //debug tracePrefix "CONNECTED %A" (client.Client.LocalEndPoint, client.Client.RemoteEndPoint)

//     //Throws
//     //ObjectDisposedException => connection was closed
//     //SocketException => something went wrong
//     let connectAsync tracePrefix (client: TcpClient) = async {
//         //debug tracePrefix "ASYNC-CONNECTING"
//         do! client.AsyncConnent endPoint
//         setConnectionOptions client

//         //debug tracePrefix "ASYNC-CONNECTED %A" (client.Client.LocalEndPoint, client.Client.RemoteEndPoint)
//     }

//     let getAvailableAsync tracePrefix = async {
//         let client = new TcpClient()
//         do! connectAsync tracePrefix client
//         return client
//     }

//     let getAvailableSync tracePrefix = 
//         let client = new TcpClient()
//         connectSync tracePrefix client
//         client

//     let addAvailableAsync tracePrefix = async {
//         let! client = getAvailableAsync tracePrefix
//         available.Enqueue client
//     }

//     let addAvailableSync tracePrefix =
//         let client = getAvailableSync tracePrefix
//         available.Enqueue client

//     let poolClient tracePrefix client = new PooledTcpClient(tracePrefix, client, self)

//     do 
//         if minConnections > maxConnections then invalidArg "minConnections" "Minimum number of connections is greater than maximum."

//         [1..minConnections] |> Seq.iter (fun _ -> addAvailableSync())
//         [1..(maxConnections - minConnections)] |> Seq.iter (fun _ -> available.Enqueue(new TcpClient()))
//         //debug String.Empty "Constructed ClientConnectionPool(minConnections = %d, maxConnections = %d, retryInterval = %d)" minConnections maxConnections retryInterval

//     new (endPoint: IPEndPoint) = new ParallelClientConnectionPool(endPoint, 0, 20, 500)

//     member internal x.AsyncTryAcquireClient(tracePrefix: string) = 
//         let debug x = debug tracePrefix x
//         async {
//             //debug "ASYNC-TRY-ACQUIRE-CLIENT"
//             let wasAvailable, freeClient = available.TryDequeue()

//             if not wasAvailable then 
//                 //debug "NO CONNECTION AVAILABLE"
//                 return None
//             else
//                 let endPoints = connectionEndPoints freeClient
//                 //debug "CONNECTION TEST %A" endPoints
//                 if not (isConnected freeClient) then
//                     //debug "CONNECTION FAULT %A" endPoints
//                     //ensure disposal
//                     freeClient.Close()
//                     //debug "CONNECTION CLOSE %A" endPoints
//                     //get new client
//                     let! newClientResult = getAvailableAsync() |> Async.Catch
//                     return match newClientResult with
//                            | Choice1Of2 newClient -> 
//                                 //debug "NEW CONNECTION %A" (connectionEndPoints newClient)
//                                 Some newClient
//                            | Choice2Of2 e -> 
//                                 //debug "NEW CONNECTION FAILURE: %A" e
//                                 available.Enqueue(new TcpClient()); raise e
//                 else
//                     //debug "CONNECTION OK %A" endPoints
//                     return Some freeClient
//         }

//     member x.AsyncTryAcquireConnection(tracePrefix: string) = async {
//         let! client = x.AsyncTryAcquireClient tracePrefix
//         return Option.bind ((poolClient tracePrefix) >> Some) client
//     }

//     member internal x.AsyncAcquireClient(tracePrefix: string) = 
//         let debug x = debug tracePrefix x
//         async {
//             latchRelease.WaitToZero()
//             latchAcquire.Increment()
//             //debug "ASYNC-ACQUIRE-CLIENT"
//             let suspended = ref false
//             try
//                 let! r = x.AsyncTryAcquireClient tracePrefix
//                 match r with
//                 | Some client -> 
//                     //debug "CLIENT ACQUIRED %A" (connectionEndPoints client)
//                     return client
//                 | None ->
//     //                debug "CONNECTION RETRY"
//     //                do! Async.Sleep retryInterval
//     //                return! x.AsyncAcquireClient tracePrefix
//                     let! ct = Async.CancellationToken
//                     return!
//                         Async.FromContinuations(fun (success: TcpClient -> unit, error, cancel) ->
//                             if ct.IsCancellationRequested then 
//                                 //debug "CANCEL CONNECTION ACQUIRE %A" endPoint
//                                 cancel (new OperationCanceledException())
//                             suspended := true
//                             //debug "CONNECTION ACQUIRE SUSPEND %A" endPoint
//                             pendingRequests.Enqueue((success, error, cancel, ct))
//                             latchAcquire.Decrement()
//                         )
//             finally
//                 if not suspended.Value then latchAcquire.Decrement()
//         }

//     member x.AsyncAcquireConnection(tracePrefix: string) = async {
//         let! r = x.AsyncAcquireClient tracePrefix
//         return poolClient tracePrefix r
//     }

//     member x.AsyncAcquireConnection() = x.AsyncAcquireConnection(String.Empty)

//     member self.ReleaseConnection(client: PooledTcpClient) =
//         let debug x = debug client.TracePrefix x
//         let cl = client.UnderlyingClient : TcpClient
//         //debug "RELEASING CONNECTION %A" (cl.Client.LocalEndPoint, cl.Client.RemoteEndPoint)
// //        available.Enqueue client.UnderlyingClient
// //        debug "CONNECTION RELEASED %A" (cl.Client.LocalEndPoint, cl.Client.RemoteEndPoint)
//         try
//             latchRelease.Increment()
//             latchAcquire.WaitToZero()
//             let isPending, pendingData = pendingRequests.TryDequeue()
//             if isPending then 
//                 //debug "CONNECTION RELEASE PENDING ACQUIRE %A %A" client.UnderlyingClient.Client.LocalEndPoint client.UnderlyingClient.Client.RemoteEndPoint
//                 let (pendingSuccess, pendingError, pendingCancel, pendingCt) = pendingData
//                 if pendingCt.IsCancellationRequested then pendingCancel (new OperationCanceledException())
//                 if not (isConnected client.UnderlyingClient) then
//                     client.UnderlyingClient.Close()
//                     try 
//                         //debug "CONNECTION RETRY NEW %A" endPoint
//                         let client' = getAvailableSync() 
//                         //debug "CONNECTION RETRY NEW RESUME ACQUIRE %A" endPoint
//                         pendingSuccess client'
//                     with e -> 
//                         //debug "CONNECTION RETRY FAULT %A %A" endPoint e
//                         available.Enqueue (new TcpClient())
//                         pendingError e
//                 else 
//                     //debug "CONNECTION ACQUIRE RESUME %A %A" cl.Client.LocalEndPoint cl.Client.RemoteEndPoint
//                     pendingSuccess client.UnderlyingClient
//             else
//                 //debug "CONNECTION RELEASED %A %A" cl.Client.LocalEndPoint cl.Client.RemoteEndPoint
//                 available.Enqueue client.UnderlyingClient
//         finally
//             latchRelease.Decrement()

//     member self.Clear() = 
//         //debug String.Empty "CLEARING"
//         for connection in available do connection.Close()

//     interface IClientConnectionPool with
//         override self.AsyncAcquireConnection(tracePrefix: string): Async<PooledTcpClient> = self.AsyncAcquireConnection(tracePrefix)
//         override self.ReleaseConnection(client: PooledTcpClient): unit = self.ReleaseConnection(client)
//         override self.Clear(): unit = self.Clear()



