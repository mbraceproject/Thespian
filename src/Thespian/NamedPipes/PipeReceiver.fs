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

type private Request<'T> =
  | Message of 'T
  | Disconnect

type private Response =
  | Acknowledge
  | UnknownRecepient
  | Error of exn

type PipeReceiver<'T>(pipeName: string, processMessage: 'T -> unit) =
  let serializer = Serialization.defaultSerializer

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
        let! data = server.AsyncReadBytes()
        try
          let msg = serializer.Deserialize<Request<'T>>(data)
          match msg with
          | Message m ->
            let response =
              try
                do processMessage m
                Acknowledge
              with :? ActorInactiveException -> UnknownRecepient
            let data = serializer.Serialize response
            do! server.AsyncWriteBytes data
            return! connectionLoop server
          | Disconnect -> return ()
        with e ->
          let data = serializer.Serialize <| Error e
          do! server.AsyncWriteBytes data
          return ()
      with e -> do! errorEvent.TriggerAsync e
    }

  let rec serverLoop server =
    async {
      try
        do! awaitConnectionAsync server
        return! connectionLoop server
        server.Disconnect()
        return! serverLoop server
      with e -> return errorEvent.Trigger e
    }

  let mutable cts = new CancellationTokenSource()
  let mutable server = createServerStreamInstance()
  do Async.Start(serverLoop server, cts.Token)

  member __.PipeName = pipeName
  member __.Errors = errorEvent.Publish
  member __.Start() =
    if cts.IsCancellationRequested then
      cts <- new CancellationTokenSource()
      server <- createServerStreamInstance()
      Async.Start(serverLoop server, cts.Token)
  member __.Stop() =
    try
      if not cts.IsCancellationRequested then
        if server.IsConnected then server.WaitForPipeDrain()
        cts.Cancel()
        if server.IsConnected then server.Disconnect()
        server.Dispose()
    with _ -> ()

  interface IDisposable with override __.Dispose() = __.Stop()


// client side implementation
type PipeSender<'T> private (pipeName: string, actorId: ActorId) =
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
      if not client.IsConnected then reply <| Exception (new UnknownRecipientException("npp: message target is stopped or pipe is broken", actorId))
      else
      try
        let data = serializer.Serialize<Request<'T>>(Message msg)
        do! client.AsyncWriteBytes data

        let! replyData = client.AsyncReadBytes()

        match serializer.Deserialize<Response> replyData with 
        | Acknowledge -> reply nothing
        | UnknownRecepient -> reply <| Exception (new UnknownRecipientException("npp: message recepient not found on remote target.", actorId))
        | Error e -> reply <| Exception (new DeliveryException("npp: message delivery failure.", actorId, e))
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
    let connectionTimeout = defaultArg connectionTimeout 10000
    let taken = ref false
    spinLock.Enter(taken)
    if isReleased then spinLock.Exit(); false
    else
      if refCount = 0 then
        try
          client.Connect(connectionTimeout)
          poster.Start()
        with :? TimeoutException as e -> spinLock.Exit(); raise <| new UnknownRecipientException("npp: unable to connect to recepient", actorId, e)
            | _ -> spinLock.Exit(); reraise()
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
        try Async.RunSynchronously (client.AsyncWriteBytes <| serializer.Serialize<Request<'T>>(Disconnect)) with _ -> ()
        client.Dispose()
        senders.TryRemove(pipeName) |> ignore
      refCount <- refCount - 1
      spinLock.Exit()

  static member GetPipeSender(pipeName: string, actorId: ActorId) =
    let sender = senders.GetOrAdd(pipeName, fun _ -> new PipeSender<'T>(pipeName, actorId))
    if sender.Acquire() then sender
    else Thread.SpinWait(20); PipeSender<'T>.GetPipeSender(pipeName, actorId)
