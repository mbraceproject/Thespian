namespace Nessos.Thespian.Remote.TcpProtocol

    open System
    open System.IO
    open System.Net
    open System.Net.Sockets
    open System.Threading
    open System.Threading.Tasks
    open System.Runtime.Serialization

    open Nessos.Thespian
    open Nessos.Thespian.AsyncExtensions
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Utils
    open Nessos.Thespian.Remote
    open Nessos.Thespian.Remote.SocketExtensions


    type RecepientProcessor = MsgId * byte[] * ProtocolStream

    type private ResultException(protocolRequest: ProtocolRequest, protocolStream: ProtocolStream) =
        inherit Exception()
        member self.Result = protocolRequest, protocolStream

    type TcpProtocolListener(ipEndPoint: IPEndPoint, serializer: IMessageSerializer, ?backLog: int, ?concurrentAccepts: int, ?connectionTimeout: int, ?maxWaitingConnections: int) as self =
        //let debug i x = Debug.writelfc (sprintf "TcpListener::%A::%A::" ipEndPoint i) x
        //default configuration
        let backLog = defaultArg backLog 50
        let concurrentAccepts = defaultArg concurrentAccepts 10
        let connectionTimeout = defaultArg connectionTimeout (int <| TimeSpan.FromSeconds(60.0).TotalMilliseconds)
        let maxWaitingConnections = defaultArg maxWaitingConnections 15

        let openConnnections = ref 0

        let registeredRecepientActors = Atom.atom Map.empty<ActorId, ActorRef<RecepientProcessor> * (RecepientProcessor -> Async<unit>)>
        let registeredRecepientProcessors = Atom.atom Map.empty<MsgId, (RecepientProcessor -> Async<unit>)>

        let logEvent = new Event<Log>()
    
        let tcpListener = new TcpListener(ipEndPoint)

//        let initReadx c stream timeout = async {
//            try
//                let! r = ProtocolStream.InitReadAsync stream
//                         |> withTimeout timeout
//                return Some r
//            with :? TimeoutException -> 
//                //sprintfn "TIMEOUT2 %d" c; 
//                return None
//        }
        let initReadx c stream timeout = ProtocolStream.InitReadAsync stream |> Async.WithTimeout timeout

        let connectionLoop keepOpen (tcpClient: TcpClient) = 
            let connectionEndPoints = (tcpClient.Client.LocalEndPoint, tcpClient.Client.RemoteEndPoint)
            //let debug x = debug connectionEndPoints x
            
            //debug "CONNECTION LOOP START"

            let networkStream = new NetworkStream(tcpClient.Client, false)
            
            let rec connectionLoopInner networkStream = async {
                try
                    //debug "CONNECTION WAITING INPUT (timeout=%d)" connectionTimeout
                    let! r = async {
                        if keepOpen then return! initReadx connectionEndPoints networkStream connectionTimeout
                        else
                            let! r = ProtocolStream.InitReadAsync networkStream
                            return Some r
                    }
                    match r with
                    | Some((msgId, actorId, payload), protocolStream) when msgId = MsgId.Empty && payload.Length = 0 ->
                        //debug "RECEIVED PROTOCOL PING"
                        //This is a protocol ping message. The pong response is an Ack for the empty msgId
                        do! protocolStream.AsyncWriteResponse(Acknowledge msgId)
                    | Some((msgId, actorId, payload), protocolStream) ->
                        //debug "RECEIVED MESSAGE %A %A" msgId actorId
//                        let v = registeredRecepientActors.Value
//                        let v2 = registeredRecepientProcessors.Value
//                        let e = ipEndPoint
                        match registeredRecepientActors.Value.TryFind actorId with
                        | Some(_, recepientProcessor) -> //This is for server side protocol processing
                            try
                                //debug "ACTOR FORWARD %A" msgId
                                do! recepientProcessor (msgId, payload, protocolStream)
                            with e -> 
                                //debug "ACTOR FORWARD FAILURE %A: %A" msgId e
                                do! protocolStream.AsyncWriteResponse(Failure(msgId, e))
                        | None ->
                            match registeredRecepientProcessors.Value.TryFind msgId with
                            | Some recepientProccessor ->
                                try
                                    //debug "PROCESSOR FORWARD %A" msgId
                                    do! recepientProccessor (msgId, payload, protocolStream)
                                with e ->
                                    //debug "PROCESSOR FORWARD FAILURE %A: %A" msgId e
                                    do! protocolStream.AsyncWriteResponse(Failure(msgId, e))
                            | None ->
                                //debug "UNKNOWN RECIPIENT %A %A" msgId actorId
                                do! protocolStream.AsyncWriteResponse(UnknownRecipient(msgId, actorId))

                        if keepOpen && protocolStream.Retain then
                            //next connection session
                            return! connectionLoopInner networkStream
                        else
                            //let le, re = if tcpClient.Client <> null then tcpClient.Client.LocalEndPoint, tcpClient.Client.RemoteEndPoint else null, null
                            //debug "CONNECTION RESET ON SESSION END"
                            networkStream.Dispose()
                            if tcpClient.Client <> null then tcpClient.Client.LingerState = new LingerOption(true, 0) |> ignore
                            tcpClient.Close()
                            Interlocked.Decrement(openConnnections) |> ignore

                            return ()
                    | None ->
                        //let le, re = if tcpClient.Client <> null then tcpClient.Client.LocalEndPoint, tcpClient.Client.RemoteEndPoint else null, null
                        //debug "CONNECTION RESET AFTER TIMEOUT"
                        //reset the connection after timeout
                        networkStream.Dispose()
                        if tcpClient.Client <> null then tcpClient.Client.LingerState = new LingerOption(true, 0) |> ignore
                        tcpClient.Close()
                        Interlocked.Decrement(openConnnections) |> ignore

                        return ()
                with e ->
                    //debug "CONNECTION LOOP FAILURE: %A" e
                    logEvent.Trigger(Warning, Protocol "tcp-listener", new SocketListenerException("Exception in connection loop.", self.LocalEndPoint, e) :> obj)
                    //let le, re = if tcpClient.Client <> null then tcpClient.Client.LocalEndPoint, tcpClient.Client.RemoteEndPoint else null, null
                    //sprintfn "ERRSRV %A %A %A" le re e
                    networkStream.Dispose()
                    tcpClient.Close()
                    Interlocked.Decrement(openConnnections) |> ignore

                    return ()
            }

            connectionLoopInner networkStream

        let rec acceptLoop () = async {
            try
                let! tcpClient = tcpListener.AsyncAcceptTcpClient()

                let! ct = Async.CancellationToken
                //debug (tcpClient.Client.LocalEndPoint, tcpClient.Client.RemoteEndPoint) "CONNECTION ACCEPT"

                Async.Start(connectionLoop true tcpClient, ct)
            with e ->
                //debug self.LocalEndPoint "ACCEPT FAILURE: %A" e
                logEvent.Trigger(Warning, Protocol "tcp-listener", new SocketListenerException("Exception in accept receive loop.", self.LocalEndPoint, e) :> obj)

            return! acceptLoop()
        }

        let acceptLoopCancellationTokenSources = [| for i in 1..concurrentAccepts -> new CancellationTokenSource() |]

        do tcpListener.Server.NoDelay <- true
           tcpListener.Start(backLog)
           for i in 1..concurrentAccepts do
               Async.Start(acceptLoop (), acceptLoopCancellationTokenSources.[i-1].Token)

//        new (endPoint: IPEndPoint, serializer: IMessageSerializer, ?backLog: int, ?concurrentAccepts: int) =
//            new TcpProtocolListener(Address.FromEndPoint(endPoint), serializer, ?backLog = backLog, ?concurrentAccepts = concurrentAccepts)

        member l.Log = logEvent.Publish

        member l.Serializer = serializer

        member l.LocalEndPoint = tcpListener.LocalEndpoint :?> IPEndPoint

        member __.IPEndPoint = ipEndPoint

        member l.RegisterRecepient(actorId: ActorId, processor: ActorRef<RecepientProcessor>, processorF: RecepientProcessor -> Async<unit>) =
            if registeredRecepientActors.Value.ContainsKey actorId then invalidOp "Recepient is already registered."

            Atom.swap registeredRecepientActors (Map.add actorId (processor, processorF))

        member l.UnregisterRecepient(actorId: ActorId) =
            if not (registeredRecepientActors.Value.ContainsKey actorId) then invalidOp "Recepient was not registered."

            Atom.swap registeredRecepientActors (Map.remove actorId)

        member l.RegisterMessageProcessor(msgId: MsgId, processF: RecepientProcessor -> Async<unit>) =
            if registeredRecepientProcessors.Value.ContainsKey msgId then invalidOp "Recepient is already registered."

            Atom.swap registeredRecepientProcessors (Map.add msgId processF)

        member l.UnregisterMessageProcessor(msgId: MsgId) =
            if not (registeredRecepientProcessors.Value.ContainsKey msgId) then invalidOp "Recepient was not registered."

            Atom.swap registeredRecepientProcessors (Map.remove msgId)

        member l.IsRecepientRegistered(actorId: ActorId) = registeredRecepientActors.Value.ContainsKey actorId

        member l.IsMessageProcessorRegistered(msgId: MsgId) = registeredRecepientProcessors.Value.ContainsKey msgId

        member l.Dispose() =
            for cts in acceptLoopCancellationTokenSources do cts.Cancel()
            tcpListener.Stop()

        interface IDisposable with
            member l.Dispose() = l.Dispose()

    and TcpListenerPool() =
        //the keys are stringified IPEndPoints
        static let tcpListeners = Atom.atom Map.empty<string, TcpProtocolListener * bool>

        static let random = new Random()
        
        [<VolatileField>]
        static let mutable hostname = Dns.GetHostName()

        //Register a listener on any address an a specific port
        static member RegisterListener(port: int, ?serializerName: string, ?backLog: int, ?concurrentAccepts: int, ?exclusive: bool) =
            TcpListenerPool.RegisterListener(new IPEndPoint(IPAddress.Any, port), ?serializerName = serializerName, ?backLog = backLog, ?concurrentAccepts = concurrentAccepts, ?exclusive = exclusive)

        //General registration
        static member RegisterListener(endPoint: IPEndPoint, ?serializerName: string, ?backLog: int, ?concurrentAccepts: int, ?exclusive: bool) =
            let serializerName = serializerNameDefaultArg serializerName
            let isExclusive = defaultArg exclusive false
            let serializer = SerializerRegistry.Resolve serializerName

            let listener = new TcpProtocolListener(endPoint, serializer, ?backLog = backLog, ?concurrentAccepts = concurrentAccepts)

            let entry = listener, isExclusive
            let key = listener.LocalEndPoint.ToString()

            Atom.swap tcpListeners (fun instance ->
                instance.Add(key, entry)
            )

        static member GetListeners(ipEndPoint: IPEndPoint, ?serializer: string) =
            let listeners = tcpListeners.Value

            //all listeners that are not marked exclusive
            let initialCandidates = 
                listeners
                |> Map.toSeq 
                |> Seq.map snd 
                |> Seq.filter (not << snd)
                |> Seq.map fst

            let addressFilteredCandidates =
                if ipEndPoint = IPEndPoint.any then 
                    //all listeners
                    initialCandidates
                else if ipEndPoint.Address = IPAddress.Any then
                    //all listeners on specific port
                    initialCandidates |> Seq.filter (fun listener -> listener.IPEndPoint.Port = ipEndPoint.Port)
                else if ipEndPoint.Port = 0 then
                    //all listeners on spefic ip
                    initialCandidates |> Seq.filter (fun listener -> listener.IPEndPoint.Address = ipEndPoint.Address)
                else
                    //completely specific allocation
                    match listeners.TryFind(ipEndPoint.ToString()) with
                    | Some (listener, _) -> Seq.singleton listener
                    | None -> Seq.empty 

            match serializer with
            | Some serializerName -> 
                addressFilteredCandidates |> Seq.filter (fun listener -> listener.Serializer.Name = serializerName)
            | None -> addressFilteredCandidates


        //General listener allocation
        static member GetListener(ipEndPoint: IPEndPoint, ?serializer: string) =
            let available = TcpListenerPool.GetListeners(ipEndPoint, ?serializer = serializer)
            if Seq.isEmpty available then
                match tcpListeners.Value.TryFind (ipEndPoint.ToString()) with
                | Some(listener, _) -> listener
                | None -> raise <| new TcpProtocolConfigurationException(sprintf "No listener registered for endpoint %O" ipEndPoint)
            else
                let index = random.Next(0, Seq.length available)

                Seq.nth index available

        static member GetListener(?serializer: string) =
            TcpListenerPool.GetListener(IPEndPoint.any, ?serializer = serializer)

        static member GetListener(port: int, ?serializer: string) =
            TcpListenerPool.GetListener(IPEndPoint.anyIp port, ?serializer = serializer)

        static member GetListener(ipAddress: IPAddress, ?serializer: string) =
            TcpListenerPool.GetListener(IPEndPoint.anyPort ipAddress, ?serializer = serializer)

        static member DefaultHostname with get() = hostname
                                       and set(newHostname: string) = hostname <- newHostname

        //This is an external facing address
        //it is what ActorRefs wiht tcp protocols carry with them as the actor's address
        //we only support IPv4 for the moment
        //if hostname is not resolved, throws SocketEcxeption
        static member IPAddresses with get() = Dns.GetHostAddresses(hostname) |> Array.filter (fun address -> address.AddressFamily = AddressFamily.InterNetwork)
        static member GetIPAddressesAsync() = async {
            let! addresses = Dns.AsyncGetHostAddresses(hostname)
            return addresses |> Array.filter (fun address -> address.AddressFamily = AddressFamily.InterNetwork)
        }

        static member Clear() = 
            tcpListeners.Value
            |> Map.toSeq |> Seq.map snd |> Seq.map fst |> Seq.iter (fun listener -> listener.Dispose())

            Atom.swap tcpListeners (fun _ -> Map.empty)

