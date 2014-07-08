namespace Nessos.Thespian.Remote.PipeProtocol

open System
open System.Collections.Concurrent
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
    member __.Bind(f: Task<'T>, g: 'T -> Async<'S>) = __.Bind(Async.AwaitTask f, g)
    member __.Bind(f: Task, g: unit -> Async<'S>) = __.Bind(f.ContinueWith ignore |> Async.AwaitTask, g)


  type Stream with
    member self.AsyncWriteBytes (bytes: byte []) =
      async {
        do! self.WriteAsync(BitConverter.GetBytes bytes.Length, 0, 4)
        do! self.WriteAsync(bytes, 0, bytes.Length)
        do! self.FlushAsync()
      }

    member self.AsyncReadBytes(length: int) =
      let rec readSegment buf offset remaining =
        async {
          let! read = self.ReadAsync(buf, offset, remaining)
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


  type Event<'T> with member self.TriggerAsync(t: 'T) = Task.Factory.StartNew(fun () -> self.Trigger t)


type private Response =
  | Acknowledge
  | Error of exn


//An IObservable wrapper for one or more named pipe server objects receiving connections asynchronously
type PipeReceiver<'T>(pipeName: string, ?singularAccept: bool) =
  let singularAccept = defaultArg singularAccept false
  let serializer = Serialization.defaultSerializer

  let receiveEvent = new Event<'T>()
  let errorEvent = new Event<exn>()

  let createServerStreamInstance _ =
    new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous)

  // avoid getting ObjectDisposedException in callback if server has already been disposed
  let awaitConnectionAsync (s: NamedPipeServerStream) =
    async {
      let! (ct: CancellationToken) = Async.CancellationToken
      return!
        Async.FromBeginEnd(s.BeginWaitForConnection,
          fun r -> 
            if ct.IsCancellationRequested then ()
            else s.EndWaitForConnection r)
    }

  let rec connectionLoop (server: NamedPipeServerStream) =
    async {
      try
        // download request
        let! reply = 
          async {
            try
              let! data = server.AsyncReadBytes()
              let msg = serializer.Deserialize<'T>(data)
              // trigger event
              let _ = receiveEvent.TriggerAsync msg
              return Acknowledge

            with e -> return Error e
          }

        // acknowledge reception to client
        let data = serializer.Serialize reply
        do! server.AsyncWriteBytes data
      with e -> let _ = errorEvent.TriggerAsync e in ()

      if singularAccept then
        server.Disconnect()
        return ()
      else return! connectionLoop server
    }

  let serverLoop server =
    async {
      try
        do! awaitConnectionAsync server
        return! connectionLoop server
      with e -> return errorEvent.Trigger e
    }

  let cts = new CancellationTokenSource()
  let server = createServerStreamInstance()
  do Async.Start(serverLoop server, cts.Token)

  member __.PipeName = pipeName
  member __.Errors = errorEvent.Publish
  member __.Stop() =
    try
      cts.Cancel ()
      server.Dispose()    
    with _ -> ()

  interface IObservable<'T> with override __.Subscribe o = receiveEvent.Publish.Subscribe o
  interface IDisposable with override __.Dispose() = __.Stop()


// client side implementation
type PipeSender<'T> private (pipeName: string) =
  let serializer = Serialization.defaultSerializer
  let client = new NamedPipeClientStream(pipeName)
  [<VolatileField>]
  let mutable refCount = 0
  [<VolatileField>]
  let mutable isReleased = false
  let spinLock = SpinLock(false)
  
  static let senders = new ConcurrentDictionary<string, PipeSender<'T>>()

  let post (R reply, msg: 'T) =
    async {
      try
        let data = serializer.Serialize<'T>(msg)
        do! client.AsyncWriteBytes data

        let! replyData = client.AsyncReadBytes() 

        match serializer.Deserialize<Response> replyData with 
        | Acknowledge -> reply nothing
        | Error e -> reply <| Exception e
      with e -> reply <| Exception e
    }

  let poster = Actor.bind <| Behavior.stateless post

  member __.PostAsync(msg: 'T) =
    async {
      try return! !poster <!- fun ch -> ch, msg
      with :? MessageHandlingException as e -> return! Async.Raise e.InnerException
    }

  member __.Post(msg: 'T) = __.PostAsync(msg) |> Async.RunSynchronously

  member private __.Acquire(?connectionTimeout: int) =
    let connectionTimeout = defaultArg connectionTimeout 30000
    let taken = ref false
    spinLock.Enter(taken)
    if isReleased then spinLock.Exit(); false
    else
      if refCount = 0 then
        try
          client.Connect(connectionTimeout)
          poster.Start()
        with _ -> spinLock.Exit(); reraise()
      refCount <- refCount + 1
      spinLock.Exit()
      true

  interface IDisposable with
    override __.Dispose() =
      let taken = ref false
      spinLock.Enter(taken)
      if refCount = 1 then
        isReleased <- true
        poster.Stop()
        client.Dispose()
        senders.TryRemove(pipeName) |> ignore
      refCount <- refCount - 1
      spinLock.Exit()

  static member GetPipeSender(pipeName: string) =
    let sender = senders.GetOrAdd(pipeName, fun _ -> new PipeSender<'T>(pipeName))
    if sender.Acquire() then sender
    else Thread.SpinWait(20); PipeSender<'T>.GetPipeSender(pipeName)
