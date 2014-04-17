namespace Nessos.Thespian.Remote.PipeProtocol

    open System
    open System.Diagnostics
    open System.Threading

    open System.Runtime.Serialization
    
    open Nessos.Thespian
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.AsyncExtensions

    //
    //  This is a *very* rudimentary implementation of an actor protocol for named pipes
    //
    //  If we are to use this in a bigger scale, here's a short list of issues that need to
    //  be addressed.
    //    1. Every actor published on this protocol creates its own named pipe, this probably
    //       doesn't scale very well. Prolly need something akin to the recipient architecture
    //       used in tcp actors.
    //    2. Also, every reply channel opens up a separate named pipe on the client side; this
    //       has the advantage of not needing to patch reply channels in the deserialization stage
    //       and you get forwarding for free. But again, this makes the protocol *really* inefficient.
    //    3. The addressing scheme is potentially restrictive. Pipe names are built exclusively out of 
    //       the process Id and the actor name.
    //    4. It has barely been tested. For the moment, this protocol is only used to bootstrap TCP
    //       connections with spawned mbraced nodes.
    //    5. Event logging is a mess.
    //
    //

    type PipeActorId internal (pipeName : string, uuid : ActorUUID, actorName : string) =
        inherit ActorId()

        let idValue = sprintf "npp/%s/%A/%s" pipeName uuid (match actorName with null | "" -> "*" | _ -> actorName)

        new(pipeName : string, actorRef : ActorRef) = new PipeActorId(pipeName, actorRef.UUId, actorRef.Name)

        override actorId.ToString() = idValue

    //
    //  reply channels
    //

    type PipedReplyChannelReceiver<'R> (actorId : PipeActorId, serializer : IMessageSerializer, timeout) =
        let chanId = Guid.NewGuid().ToString()
        let replyReceiver = new PipeReceiver<Reply<'R>> (serializer = serializer, pipeName = chanId, singularAccept = true)
        let await = Async.AwaitObservable (replyReceiver, timeout)

        member __.AwaitReply() = 
            async {
                try return! await
                with e ->
                    return! Async.Raise <|
                        CommunicationException(sprintf "PipeProtocol: error receiving reply from %O." actorId, e)
            }

        // pubishes a serialiable descriptor for this receiver
        member __.ReplyChannel = PipedReplyChannel<'R>(chanId, serializer, timeout)
        interface IDisposable with 
            member __.Dispose () = 
                replyReceiver.Stop ()

    and PipedReplyChannel<'R> internal (chanId : string, serializer : IMessageSerializer, timeout) =
        let mutable timeout = timeout
        
        let reply v =
            try
                let client = PipeSender<Reply<'R>>(chanId, serializer)
                client.Post(v, connectionTimeout = timeout)
            with e -> raise <| CommunicationException("PipeProtocol: cannot reply.", e)

        new (sI : SerializationInfo, _ : StreamingContext) =
            let chanId = sI.GetString("chanId")
            let serializerName = sI.GetString("serializerName")
            let timeout = sI.GetInt32("timeout")
            let serializer = SerializerRegistry.Resolve serializerName
            new PipedReplyChannel<'R>(chanId, serializer, timeout)

        interface IReplyChannel<'R> with
            member __.Protocol = "npp"
            member __.Timeout with get () = timeout and set t = timeout <- t
            member __.ReplyUntyped (v : Reply<obj>) = reply (Reply.unbox v)
            member __.Reply (v : Reply<'R>) = reply v
            member __.WithTimeout t = new PipedReplyChannel<'R>(chanId, serializer, t) :> IReplyChannel<'R>

        interface ISerializable with
            member __.GetObjectData(sI : SerializationInfo, _ : StreamingContext) =
                sI.AddValue("chanId", chanId)
                sI.AddValue("serializerName", serializer.Name)
                sI.AddValue("timeout", timeout)

    //
    //  the protocol
    //

    type PipeProtocolServer<'T> (pipeName : string, serializer : IMessageSerializer, actorRef : ActorRef<'T>) =
        let server = new PipeReceiver<'T * IReplyChannel * bool>(pipeName, serializer)

        let forwarderErrors = new Event<exn> ()
        let errorEvent = Event.merge forwarderErrors.Publish server.Errors

        let forwarder =
            let rec behaviour (self : Actor<'T * IReplyChannel * bool>) =
                let reportError e = try forwarderErrors.Trigger e with _ -> ()
                async {
                    let! msg, (r : IReplyChannel), withReply = self.Receive ()

                    let! response = actorRef.PostAsync msg |> Async.Catch

                    try
                        match response with
                        | Choice1Of2 () when withReply -> ()
                        //ack that remote actor has received message
                        | Choice1Of2 () -> r.ReplyUntyped <| Reply.box nothing
                        | Choice2Of2 e -> 
                            r.ReplyUntyped <| Exception e
                            reportError e
                    with e -> reportError e

                    return! behaviour self
                }

            Actor.bind behaviour |> Actor.start

        do server.Add forwarder.Ref.Post

        member s.Errors = errorEvent
        member s.ActorRef = actorRef
        member s.Stop () = 
            forwarder.Stop ()
            server.Stop ()

        interface IDisposable with
            member s.Dispose () = s.Stop ()

    type PipeProtocol<'T> private (config : PipeProtocolConfig, pipeName : string, 
                                        serializer : IMessageSerializer, actorName : string,
                                        actorUUID : ActorUUID, server : PipeProtocolServer<'T> option) =

        static let mkPipeName (config : PipeProtocolConfig) actorName =
            sprintf "pid-%d-actor-%s" config.Pid actorName

        let actorId = PipeActorId(pipeName, actorUUID, actorName)

        // whatever ..
        let eventLog = new Event<Log> ()
        let events =
            match server with
            | Some server -> 
                server.Errors 
                |> Event.map(fun e -> LogLevel.Error, LogSource.Protocol "npp", e :> obj)
                |> Event.merge eventLog.Publish
            | None -> eventLog.Publish

        let sender = new PipeSender<'T * IReplyChannel * bool>(pipeName, serializer)

        let post (msg : 'T) =
            async {
                use rcr = new PipedReplyChannelReceiver<unit>(actorId, serializer, Timeout.Infinite)

                try
                    do! sender.PostAsync <| (msg, rcr.ReplyChannel :> _, false)
                with e ->
                    return! Async.Raise <|
                        CommunicationException(sprintf "PipeProtocol: error communicating with %O." actorId, e)

                let! response = rcr.AwaitReply ()

                match response with
                | Value () -> return ()
                | Exception e -> return! Async.Raise e
            }

        let postWithReply (msgB : IReplyChannel<'R> -> 'T, timeout) =
            async {
                use rcr = new PipedReplyChannelReceiver<'R>(actorId, serializer, timeout)
                
                let rc = rcr.ReplyChannel

                try
                    do! sender.PostAsync((msgB rc, rc :> _, true), connectionTimeout = timeout)
                with e ->
                    return! Async.Raise <|
                        CommunicationException(sprintf "PipeProtocol: error communicating with %O" actorId, e)

                return! rcr.AwaitReply ()
            }

        new (config : PipeProtocolConfig, actorRef : ActorRef<'T>) =
            let pipeName = mkPipeName config actorRef.Name
            let server = new PipeProtocolServer<'T>(pipeName, config.Serializer, actorRef)
            new PipeProtocol<'T>(config, pipeName, config.Serializer, actorRef.Name, actorRef.UUId, Some server) 

        new (config : PipeProtocolConfig, actorUUID : ActorUUID, actorName : string) =
            let pipeName = mkPipeName config actorName
            new PipeProtocol<'T>(config, pipeName, config.Serializer, actorName, actorUUID, None)

        interface IActorProtocol<'T> with
            member __.ActorId = actorId :> _
            member __.ActorUUId = actorUUID
            member __.ActorName = actorName
            member __.MessageType = typeof<'T>
            member __.ProtocolName = "npp"
            member __.Log = events
            member __.Configuration = Some (config :> IProtocolConfiguration)
            member __.Start () = ()
            member __.Stop () = server |> Option.iter (fun s -> s.Stop())
            member __.Post msg = Async.RunSynchronously(post msg)
            member __.PostAsync msg = post msg
            member __.PostWithReply<'R> (msgB, timeout) : Async<'R> = 
                async {
                    let! r = postWithReply(msgB, timeout)
                    match r with
                    | Value v -> return v
                    | Exception e -> return! Async.Raise e
                }
            member __.TryPostWithReply<'R> (msgB, timeout) : Async<'R option> =
                async {
                    let! r = postWithReply(msgB, timeout)
                    match r with
                    | Value v -> return Some v
                    | Exception _ -> return None
                }
            member __.PostWithReply<'R> msg : Async<'R> =
                async {
                    let! r = postWithReply (msg, Timeout.Infinite)
                    match r with
                    | Value v -> return v
                    | Exception e -> return! Async.Raise e
                }


    and PipeProtocolConfig(?proc : Process, ?serializer : IMessageSerializer) =
        let isServer = proc.IsNone
        let proc = match proc with None -> Process.GetCurrentProcess () | Some p -> p
        let serializer = 
            match serializer with
            | None -> SerializerRegistry.GetDefaultSerializer()
            | Some s -> s
        
        let mkPipeName (actorName : string) =
            sprintf "pid-%d-actor-%s" proc.Id actorName

        let compareTo (y : obj) =
            match y with
            | :? PipeProtocolConfig as y -> compare proc.Id y.Pid
            | :? IProtocolConfiguration as y -> compare "npp" y.ProtocolName
            | _ -> invalidArg "y" "invalid comparand"
        
        member __.Pid = proc.Id
        member __.Serializer = serializer

        member s.GetClientInstance<'T> (uuid : ActorUUID, name : string) =
            new PipeProtocol<'T>(s, uuid, name)

        interface IProtocolConfiguration with
            member c.ProtocolName = "npp"
            member c.Serializer = None
            member c.CreateProtocolInstances<'T> (actorRef : ActorRef<'T>) =
                if isServer then
                    [| new PipeProtocol<'T>(c, actorRef) :> IActorProtocol<'T> |]
                else
                    invalidOp "pipe protocol configuration is not server mode."
            member c.CreateProtocolInstances<'T> (uuid : ActorUUID, name : string) = 
                [| new PipeProtocol<'T>(c, uuid, name) :> IActorProtocol<'T> |]
//            member c.TryCreateProtocolInstances (uuid: ActorUUID, name : string) =
//                try Some (new PipeProtocol<'T>(c, uuid, name) :> IActorProtocol<'T>)
//                with _ -> None
            
            member x.CompareTo (y : obj) = compareTo y
            member x.CompareTo (y : IProtocolConfiguration) = compareTo y


    type PipeProtocol = PipeProtocolConfig
                

    
    module ActorRef =
        let ofProcess<'T> (proc : Process) (actorName : string) =
            let protoConf = new PipeProtocol(proc) :> IProtocolConfiguration
            let proto = protoConf.CreateProtocolInstances<'T>(Guid.Empty, actorName)
            new ActorRef<'T>(proto.[0].ActorUUId, proto.[0].ActorName, proto)
            
        let ofProcessId<'T> (pid : int) (name : string) =
            ofProcess<'T> (Process.GetProcessById pid) name
        