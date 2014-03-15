namespace Thespian.Remote.PipeProtocol

    open System
    open System.IO
    open System.IO.Pipes
    open System.Threading
    
    open Thespian
    open Thespian.AsyncExtensions
    open Thespian.Serialization
    open Thespian.Utils

    module internal Utils =
        // option types don't play well with binary formatter
        type Response = Ack | Error of exn

        // TODO : add async support to ISerializer?
        let serializeAsync (serializer : IMessageSerializer) (stream : Stream) (o : obj) =
            async {
                let bytes = serializer.Serialize(null, o)

                do! stream.AsyncWrite(BitConverter.GetBytes bytes.Length)
                do! stream.AsyncWrite(bytes)

                do! stream.FlushAsync() |> Async.AwaitTask
            }

        let deserializeAsync<'T> (serializer : IMessageSerializer) (stream : Stream) =
            async {
                let! lbytes = stream.AsyncRead(sizeof<int>)
                let! bytes = stream.AsyncRead(BitConverter.ToInt32(lbytes,0))

                return serializer.Deserialize(null, bytes) :?> 'T
            }


    open Utils

    // An IObservable wrapper for one or more named pipe server objects receiving connections asynchronously

    type PipeReceiver<'T>(pipeName : string, serializer : IMessageSerializer, ?singularAccept, ?concurrentAccepts) =
        let singularAccept = defaultArg singularAccept false
        let concurrentAccepts = if singularAccept then 1 else defaultArg concurrentAccepts 1

        let receiveEvent = new Event<'T> ()
        let errorEvent = new Event<exn> ()

        do if concurrentAccepts < 1 then invalidArg "concurrentAccepts" "must be positive value."

        let createServerStreamInstance _ =
            new NamedPipeServerStream(pipeName, PipeDirection.InOut, concurrentAccepts, 
                                            PipeTransmissionMode.Byte, PipeOptions.Asynchronous)

        // avoid getting ObjectDisposedException in callback if server has already been disposed
        let awaitConnectionAsync (s : NamedPipeServerStream) =
            async {
                let! ct = Async.CancellationToken
                return!
                    Async.FromBeginEnd(s.BeginWaitForConnection,
                        fun r -> 
                            if ct.IsCancellationRequested then ()
                            else
                                s.EndWaitForConnection r)
                }

        let rec serverLoop (server : NamedPipeServerStream) =
            let catch f x = try f x with _ -> ()

            async {
                // wait for connection from client
                do! awaitConnectionAsync server

                // download message from client
                let! result = deserializeAsync<'T> serializer server |> Async.Catch

                match result with
                | Choice1Of2 msg ->
                    try
                        // acknowledge reception to client
                        try do! serializeAsync serializer server Ack
                        finally catch server.Disconnect ()

                        // only trigger after transaction has completed
                        catch receiveEvent.Trigger msg

                    with e ->
                        catch errorEvent.Trigger e

                    if singularAccept then return ()
                    else return! serverLoop server
                | Choice2Of2 e ->
                    // log error
                    catch errorEvent.Trigger e
                    
                    // respond with error
                    try do! serializeAsync serializer server (Error e)
                    with _ -> ()

                    catch server.Disconnect ()

                    return! serverLoop server
            }

        let cts = new CancellationTokenSource()
        let servers = [1..concurrentAccepts] |> List.map createServerStreamInstance
        // not sure if Async.Parallel is better
        do for server in servers do Async.Start(serverLoop server, cts.Token)

        member __.PipeName = pipeName
        member __.Errors = errorEvent.Publish
        member __.Stop () =
            try
                cts.Cancel ()
                for s in servers do s.Dispose()    
            with _ -> ()

        interface IObservable<'T> with
            member __.Subscribe o = receiveEvent.Publish.Subscribe o

        interface IDisposable with member __.Dispose () = __.Stop ()

    // client side implementation
    type PipeSender<'T> (pipeName : string, serializer : IMessageSerializer) =

        member __.PostAsync (msg : 'T, ?connectionTimeout) =
            let connectionTimeout = defaultArg connectionTimeout 10
            async {
                use client = new NamedPipeClientStream(pipeName)
                do client.Connect (connectionTimeout)

                do! serializeAsync serializer client msg

                let! reply = deserializeAsync<Response> serializer client

                match reply with 
                | Ack -> return () 
                | Error e -> return! Async.Raise e
            }

        member __.Post (msg : 'T, ?connectionTimeout) = 
            __.PostAsync (msg, ?connectionTimeout = connectionTimeout) |> Async.RunSynchronously