namespace Nessos.Thespian.Remote.PipeProtocol

    open System
    open System.IO
    open System.IO.Pipes
    open System.Threading
    open System.Threading.Tasks
    
    open Nessos.Thespian
    open Nessos.Thespian.AsyncExtensions
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Utils

    [<AutoOpen>]
    module private Utils =

        type AsyncBuilder with
            member __.Bind(f : Task<'T>, g : 'T -> Async<'S>) = __.Bind(Async.AwaitTask f, g)
            member __.Bind(f : Task, g : unit -> Async<'S>) = __.Bind(f.ContinueWith ignore |> Async.AwaitTask, g)

        type Stream with
            member s.AsyncWriteBytes (bytes : byte []) =
                async {
                    do! s.WriteAsync(BitConverter.GetBytes bytes.Length, 0, 4)
                    do! s.WriteAsync(bytes, 0, bytes.Length)
                    do! s.FlushAsync()
                }

            member s.AsyncReadBytes(length : int) =
                let rec readSegment buf offset remaining =
                    async {
                        let! read = s.ReadAsync(buf, offset, remaining)
                        if read < remaining then
                            return! readSegment buf (offset + read) (remaining - read)
                        else
                            return ()
                    }

                async {
                    let bytes = Array.zeroCreate<byte> length
                    do! readSegment bytes 0 length
                    return bytes
                }

            member s.AsyncReadBytes() =
                async {
                    let! lengthArr = s.AsyncReadBytes 4
                    let length = BitConverter.ToInt32(lengthArr, 0)
                    return! s.AsyncReadBytes length
                }

        type Event<'T> with
            member e.TriggerAsync(t : 'T) =
                Task.Factory.StartNew(fun () -> e.Trigger t)


    type private Response =
        | Acknowledge
        | Error of exn

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
        let awaitConnectionAsync (s : NamedPipeServerStream) = async {
            let! (ct : CancellationToken) = Async.CancellationToken
            return!
                Async.FromBeginEnd(s.BeginWaitForConnection,
                    fun r -> 
                        if ct.IsCancellationRequested then ()
                        else
                            s.EndWaitForConnection r)
        }

        let rec serverLoop (server : NamedPipeServerStream) = async {
            try
                // await connection
                do! awaitConnectionAsync server

                // download request
                let! reply = 
                    async {
                        try
                            let! data = server.AsyncReadBytes()
                            let msg = serializer.Deserialize<'T>(data)
                            // trigger event
                            let _ = receiveEvent.TriggerAsync msg
                            return Acknowledge

                        with e -> 
                            return Error e
                    }

                // acknowledge reception to client
                let data = serializer.Serialize reply
                try do! server.AsyncWriteBytes data
                finally server.Disconnect ()

            with e -> let _ = errorEvent.TriggerAsync e in ()

            if singularAccept then return ()
            else return! serverLoop server
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

                let data = serializer.Serialize<'T>(msg)
                do! client.AsyncWriteBytes data

                let! replyData = client.AsyncReadBytes () 

                match serializer.Deserialize<Response> replyData with 
                | Acknowledge -> return () 
                | Error e -> return! Async.Raise e
            }

        member __.Post (msg : 'T, ?connectionTimeout) = 
            __.PostAsync (msg, ?connectionTimeout = connectionTimeout) |> Async.RunSynchronously