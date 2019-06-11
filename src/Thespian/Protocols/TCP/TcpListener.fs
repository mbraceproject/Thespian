namespace Nessos.Thespian.Remote.TcpProtocol

open System
open System.Collections.Concurrent
open System.Net
open System.Net.Sockets
open System.Threading

open Nessos.Thespian
open Nessos.Thespian.Utils
open Nessos.Thespian.Logging
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.SocketExtensions

type RequestData = MsgId * TcpActorId * byte [] * ProtocolStream

type TcpProtocolListener(ipEndPoint : IPEndPoint, ?backLog : int, ?concurrentAccepts : int, ?connectionTimeout : int, ?maxWaitingConnections : int) as self = 
    //default configuration
    let backLog = defaultArg backLog 50
    let concurrentAccepts = defaultArg concurrentAccepts 10
    let connectionTimeout = defaultArg connectionTimeout (int <| TimeSpan.FromSeconds(5.0).TotalMilliseconds)
    let maxWaitingConnections = defaultArg maxWaitingConnections 15
    let openConnections = ref 0
    let recipientRegistry = new ConcurrentDictionary<TcpActorId, RequestData -> Async<bool>>()
    let logEvent = new Event<Log>()
    let tcpListener = new TcpListener(ipEndPoint)
    
    let processRequest (request : ((MsgId * TcpActorId * byte []) * ProtocolStream) option) = 
        async { 
            try 
                match request with
                | Some((msgId, actorId, payload), protocolStream) when msgId = MsgId.Empty && payload.Length = 0 -> 
                    try 
                        use _ = protocolStream.Acquire()
                        //protocol ping messages do not keep the connection open
                        do! Async.Ignore <| protocolStream.TryAsyncWriteResponse(Acknowledge msgId)
                    with e -> 
                        logEvent.Trigger
                            (Warning, Protocol "tcp-listener", 
                             new SocketListenerException("Exception in connection loop.", self.LocalEndPoint, e) :> obj)
                    return false
                | Some((msgId, actorId, payload), protocolStream) -> 
                    try 
                        use _ = protocolStream
                        let isValid, recipientF = recipientRegistry.TryGetValue(actorId)
                        if isValid then 
                            try 
                                return! recipientF (msgId, actorId, payload, protocolStream)
                            with e ->
                                try
                                    let! r' = protocolStream.TryAsyncWriteResponse(Failure(msgId, e))
                                    match r' with
                                    | Some() -> return true
                                    | None -> return false
                                with :? ThespianSerializationException as e' ->
                                    let e'' = new ThespianSerializationException(e.Message, SerializationOperation.Deserialization, new ThespianSerializationException(e'.Message, SerializationOperation.Serialization))
                                    let! r' = protocolStream.TryAsyncWriteResponse(Failure(msgId, e''))
                                    match r' with
                                    | Some() -> return true
                                    | None -> return false
                        else 
                            let! r' = protocolStream.TryAsyncWriteResponse(UnknownRecipient(msgId, actorId))
                            match r' with
                            | Some() -> return true
                            | None -> return false
                    with e -> 
                        logEvent.Trigger
                            (Warning, Protocol "tcp-listener", 
                             new SocketListenerException("Exception in connection loop.", self.LocalEndPoint, e) :> obj)
                        protocolStream.Acquire().Dispose()
                        return false
                | None -> return false
            with e -> 
                logEvent.Trigger
                    (Warning, Protocol "tcp-listener", 
                     new SocketListenerException("Exception in connection loop.", self.LocalEndPoint, e) :> obj)
                return false
        }
    
    let rec connectionLoop (keepOpen : bool) (tcpClient : TcpClient) = 
        async {
            try
                let! request = ProtocolStream.AsyncCreateRead(tcpClient, connectionTimeout, connectionTimeout, keepOpen)
                let! keep = processRequest request
                if keepOpen && keep then return! connectionLoop keepOpen tcpClient
                else 
                    Interlocked.Decrement(openConnections) |> ignore
                    match request with
                    | Some(_, protocolStream) -> protocolStream.Acquire().Dispose()
                    | _ -> ()
                    return ()
            with e ->
                logEvent.Trigger
                    (Warning, Protocol "tcp-listener", 
                     new SocketListenerException("Exception in connection loop.", self.LocalEndPoint, e) :> obj)
        }
    
    let rec acceptLoop() = 
        async { 
            try 
                let! tcpClient = tcpListener.AsyncAcceptTcpClient()
                let! ct = Async.CancellationToken
                Async.Start(connectionLoop true tcpClient, ct)
                Interlocked.Increment(openConnections) |> ignore
            with e -> 
                logEvent.Trigger
                    (Warning, Protocol "tcp-listener", 
                     new SocketListenerException("Exception in accept receive loop.", self.LocalEndPoint, e) :> obj)
            return! acceptLoop()
        }
    
    let acceptLoopCancellationTokenSources = 
        [| for i in 1..concurrentAccepts -> new CancellationTokenSource() |]
    
    do 
        tcpListener.Server.NoDelay <- true
        tcpListener.Start(backLog)
        for i in 1..concurrentAccepts do
            Async.Start(acceptLoop(), acceptLoopCancellationTokenSources.[i - 1].Token)
    
    member __.Log = logEvent.Publish
    member __.LocalEndPoint = tcpListener.LocalEndpoint :?> IPEndPoint
    member __.IPEndPoint = ipEndPoint
    
    member __.RegisterRecipient(actorId : TcpActorId, processorF : RequestData -> Async<bool>) = 
        if not <| recipientRegistry.TryAdd(actorId, processorF) then invalidOp "Recipient is already registered."
    
    member __.UnregisterRecipient(actorId : TcpActorId) = 
        let isRemoved, _ = recipientRegistry.TryRemove(actorId)
        if not isRemoved then invalidOp "Recipient was not registered."
    
    member __.IsRecipientRegistered(actorId : TcpActorId) = recipientRegistry.ContainsKey(actorId)
    
    member __.Dispose() = 
        for cts in acceptLoopCancellationTokenSources do
            cts.Cancel()
        tcpListener.Stop()
    
    interface IDisposable with
        member self.Dispose() = self.Dispose()

type TcpListenerPool() = 
    //the keys are stringified IPEndPoints
    static let tcpListeners = new ConcurrentDictionary<string, TcpProtocolListener>()
    static let random = new Random()
    
    [<VolatileField>]
    static let mutable hostname = Dns.GetHostName()
    
    static let initDefaultIfEmpty() = 
        if tcpListeners.IsEmpty then TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, 0))
    
    //Register a listener on any address an a specific port
    static member RegisterListener(port : int, ?backLog : int, ?concurrentAccepts : int) = 
        TcpListenerPool.RegisterListener
            (new IPEndPoint(IPAddress.Any, port), ?backLog = backLog, ?concurrentAccepts = concurrentAccepts)
    
    //General registration
    static member RegisterListener(endPoint : IPEndPoint, ?backLog : int, ?concurrentAccepts : int) = 
        let listener = new TcpProtocolListener(endPoint, ?backLog = backLog, ?concurrentAccepts = concurrentAccepts)
        let key = listener.LocalEndPoint.ToString()
        if not <| tcpListeners.TryAdd(key, listener) then invalidOp "Listener already registered on endpoint."
    
    //General listener allocation
    static member GetListeners(ipEndPoint : IPEndPoint) = 
        initDefaultIfEmpty()
        let initialCandidates = tcpListeners |> Seq.map (fun kv -> kv.Value)
        
        let addressFilteredCandidates = 
            if ipEndPoint = IPEndPoint.any then initialCandidates
            else if ipEndPoint.Address = IPAddress.Any then 
                //all listeners on specific port
                initialCandidates |> Seq.filter (fun listener -> listener.IPEndPoint.Port = ipEndPoint.Port)
            else if ipEndPoint.Port = 0 then 
                //all listeners on spefic ip
                initialCandidates |> Seq.filter (fun listener -> listener.IPEndPoint.Address = ipEndPoint.Address)
            else 
                //completely specific allocation
                let exists, listener = tcpListeners.TryGetValue(ipEndPoint.ToString())
                if exists then Seq.singleton listener
                else Seq.empty
        addressFilteredCandidates
    
    static member GetListener(ipEndPoint : IPEndPoint) = 
        let available = TcpListenerPool.GetListeners(ipEndPoint)
        if Seq.isEmpty available then 
            raise <| new TcpProtocolConfigurationException(sprintf "No listener registered for endpoint %O" ipEndPoint)
        else 
            let index = random.Next(0, Seq.length available)
            Seq.item index available
    
    static member GetListener() = TcpListenerPool.GetListener(IPEndPoint.any)
    static member GetListener(port : int) = TcpListenerPool.GetListener(IPEndPoint.anyIp port)
    static member GetListener(ipAddress : IPAddress) = TcpListenerPool.GetListener(IPEndPoint.anyPort ipAddress)
    
    static member DefaultHostname 
        with get () = hostname
        and set (newHostname : string) = hostname <- newHostname
    
    //This is an external facing address
    //we only support IPv4 for the moment
    //if hostname is not resolved, throws SocketEcxeption
    static member IPAddresses = 
        Dns.GetHostAddresses(hostname) 
        |> Array.filter (fun address -> address.AddressFamily = AddressFamily.InterNetwork)
    static member GetIPAddressesAsync() = 
        async { let! addresses = Dns.AsyncGetHostAddresses(hostname)
                return addresses |> Array.filter (fun address -> address.AddressFamily = AddressFamily.InterNetwork) }
    //Not thread safe
    //if concurrent registrations occur then there is a memory leak
    //and the pool is not cleared completely
    static member Clear() = 
        tcpListeners |> Seq.iter (fun kv -> kv.Value.Dispose())
        tcpListeners.Clear()
