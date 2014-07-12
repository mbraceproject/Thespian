namespace Nessos.Thespian.Remote.PipeProtocol

open System
open System.Collections.Concurrent
open System.IO
open System.IO.Pipes
open System.Threading
open System.Threading.Tasks

//when in unix, use unixpipes instead of .net named pipes
open Mono.Unix
open Mono.Unix.Native

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

[<AbstractClass>]
type PipeReceiver<'T>(pipeName: string) =
  static let onUnix =
    let p = (int)System.Environment.OSVersion.Platform
    p = 4 || p = 6 || p = 128
    
  member __.PipeName = pipeName

  abstract Errors: IEvent<exn>
  abstract Start: unit -> unit
  abstract Stop: unit -> unit

  interface IDisposable with override __.Dispose() = __.Stop()

  static member Create(pipeName: string, processMessage: 'T -> unit, ?singleAccept: bool) =
    if onUnix then new PipeReceiverUnix<'T>(pipeName, processMessage, ?singleAccept = singleAccept) :> PipeReceiver<'T>
    else new PipeReceiverWindows<'T>(pipeName, processMessage, ?singleAccept = singleAccept) :> PipeReceiver<'T>

and PipeReceiverUnix<'T>(pipeName: string, processMessage: 'T -> unit, ?singleAccept: bool) as self =
  inherit PipeReceiver<'T>(pipeName)

  let singleAccept = defaultArg singleAccept false
  let serializer = Serialization.defaultSerializer

  let errorEvent = new Event<exn>()

  let tmp = Path.GetTempPath()
  //tmp should have a trailing slash
  let writeFifoName = tmp + pipeName + "w"
  let readFifoName = tmp + pipeName + "r"
  let fifoPerms = FilePermissions.S_IWUSR|||FilePermissions.S_IRUSR|||FilePermissions.S_IRGRP|||FilePermissions.S_IWGRP

  let createPipes() =
    let r = Syscall.mkfifo(readFifoName, fifoPerms)
    if r = -1 then failwith (sprintf "Failed to create read end of server pipe. Error code %A" <| Stdlib.GetLastError())
    let r = Syscall.mkfifo(writeFifoName, fifoPerms)
    if r = -1 then
      Syscall.unlink(readFifoName) |> ignore
      failwith (sprintf "Failed to create write end of server pipe. Error code %A" <| Stdlib.GetLastError())

  let destroyPipes() =
    Syscall.unlink(writeFifoName) |> ignore
    Syscall.unlink(readFifoName) |> ignore


  let rec connect() =
    let readFd = Syscall.``open``(readFifoName, OpenFlags.O_RDONLY)
    if readFd = -1 then
      let errno = Stdlib.GetLastError()
      if errno = Errno.EINTR then connect()
      elif errno = Errno.ENOENT then None
      else failwith (sprintf "Failed to open read end of server pipe. Error code %A" errno)
    else
    //ownership of readFd passes to readStream
    Some <| new UnixStream(readFd)

  let rec getWriting() =
    let writeFd = Syscall.``open``(writeFifoName, OpenFlags.O_WRONLY)
    if writeFd = -1 then
      let errno = Stdlib.GetLastError()
      if errno = Errno.EINTR then getWriting()
      else failwith (sprintf "Failed to open write end of client pipe. Error code %A" errno)
    else
    new UnixStream(writeFd)    

  let rec connectionLoop (reading: Stream) =
    async {
      try
        let! data = reading.AsyncReadBytes()
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
            use writing = getWriting()
            do! writing.AsyncWriteBytes data
            return! connectionLoop reading
          | Disconnect -> return true
        with e ->
          let data = serializer.Serialize <| Error e
          use writing = getWriting()
          do! writing.AsyncWriteBytes data
          return true
      with e ->
        do! errorEvent.TriggerAsync e
        return false
    }

  let rec serverLoop () =
    async {
      try
        match connect() with
        | Some reading ->
          let! keep = connectionLoop reading
          if singleAccept then reading.Dispose(); self.Stop()
          elif keep then
            reading.Dispose()
            return! serverLoop()
          else reading.Dispose(); self.Stop()
        | None -> () //the actor was stopped
      with e -> errorEvent.Trigger e
    }

  let mutable cts = Unchecked.defaultof<CancellationTokenSource>

  override __.Errors = errorEvent.Publish
  override __.Start() =
    if cts = Unchecked.defaultof<CancellationTokenSource> then
      createPipes()
      cts <- new CancellationTokenSource()
      Async.Start(serverLoop(), cts.Token)
  override __.Stop() =
    if cts <> Unchecked.defaultof<CancellationTokenSource> then
      cts.Cancel()
      cts <- Unchecked.defaultof<CancellationTokenSource>
      destroyPipes()


and PipeReceiverWindows<'T>(pipeName: string, processMessage: 'T -> unit, ?singleAccept: bool) as self =
  inherit PipeReceiver<'T>(pipeName)
  
  let singleAccept = defaultArg singleAccept false
  let serializer = Serialization.defaultSerializer

  let errorEvent = new Event<exn>()

  let createServerStreamInstanceWindows _ =
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
          | Disconnect -> return true
        with e ->
          let data = serializer.Serialize <| Error e
          do! server.AsyncWriteBytes data
          return true
      with e ->
        do! errorEvent.TriggerAsync e
        return false
    }

  let rec serverLoop server =
    async {
      try
        do! awaitConnectionAsync server
        let! keep = connectionLoop server
        if singleAccept then self.Stop()
        elif keep then
          server.Disconnect()
          return! serverLoop server
        else self.Stop()
      with e ->
        errorEvent.Trigger e
        self.Stop()
    }

  let mutable cts = Unchecked.defaultof<CancellationTokenSource>
  let mutable server = Unchecked.defaultof<NamedPipeServerStream>

  override __.Errors = errorEvent.Publish
  override __.Start() =
    if cts = Unchecked.defaultof<CancellationTokenSource> then
      cts <- new CancellationTokenSource()
      server <- createServerStreamInstanceWindows()
      Async.Start(serverLoop server, cts.Token)
  override __.Stop() =
    try
      if cts <> Unchecked.defaultof<CancellationTokenSource> then
        if server.IsConnected then server.WaitForPipeDrain()
        cts.Cancel()
        if server.IsConnected then server.Disconnect()
        server.Dispose()
        cts <- Unchecked.defaultof<CancellationTokenSource>
        server <- Unchecked.defaultof<NamedPipeServerStream>
    with _ -> ()


// client side implementation
[<AbstractClass>]
type internal PipeSender<'T> internal (pipeName: string, actorId: ActorId) =
  static let onUnix =
    let p = (int)System.Environment.OSVersion.Platform
    p = 4 || p = 6 || p = 128
    
  let serializer = Serialization.defaultSerializer

  [<VolatileField>]
  let mutable refCount = 0
  [<VolatileField>]
  let mutable isReleased = false
  let spinLock = SpinLock(false)

  static let senders = new ConcurrentDictionary<string, PipeSender<'T>>()

  abstract Poster: Actor<IReplyChannel<unit> * 'T>
  abstract Connect: int -> unit
  abstract Disconnect: unit -> unit

  member self.PostAsync(msg: 'T) =
    async {
      try return! !self.Poster <!- fun ch -> ch.WithTimeout(Timeout.Infinite), msg
      with :? MessageHandlingException as e -> return! Async.Raise e.InnerException
    }

  member __.Post(msg: 'T) = __.PostAsync(msg) |> Async.RunSynchronously

  member private self.Acquire(?connectionTimeout: int) =
    let connectionTimeout = defaultArg connectionTimeout 10000
    let taken = ref false
    spinLock.Enter(taken)
    if isReleased then spinLock.Exit(); false
    else
      if refCount = 0 then
        try
          self.Connect(connectionTimeout)
          self.Poster.Start()
        with :? TimeoutException as e -> spinLock.Exit(); raise <| new UnknownRecipientException("npp: unable to connect to recepient", actorId, e)
            | _ -> spinLock.Exit(); reraise()
      refCount <- refCount + 1
      spinLock.Exit()
      true

  interface IDisposable with
    override self.Dispose() =
      let taken = ref false
      spinLock.Enter(taken)
      if refCount = 1 then
        isReleased <- true
        self.Poster.Stop()
        self.Disconnect()
        senders.TryRemove(pipeName) |> ignore
      refCount <- refCount - 1
      spinLock.Exit()

  static member GetPipeSender(pipeName: string, actorId: ActorId) =
    let sender = senders.GetOrAdd(pipeName, fun _ -> if onUnix then new PipeSenderUnix<'T>(pipeName, actorId) :> PipeSender<'T> else new PipeSenderWindows<'T>(pipeName, actorId) :> PipeSender<'T>)
    if sender.Acquire() then sender
    else Thread.SpinWait(20); PipeSender<'T>.GetPipeSender(pipeName, actorId)


and internal PipeSenderUnix<'T> internal (pipeName: string, actorId: ActorId) =
  inherit PipeSender<'T>(pipeName, actorId)
  
  let serializer = Serialization.defaultSerializer
  let tmp = Path.GetTempPath()
  //fifo names are reversed from server's
  let writeFifoName = tmp + pipeName + "r"
  let readFifoName = tmp + pipeName + "w"

  let rec getReading() =
    let readFd = Syscall.``open``(readFifoName, OpenFlags.O_RDONLY)
    if readFd = -1 then
      let errno = Stdlib.GetLastError()
      if errno = Errno.EINTR then getReading()
      else failwith (sprintf "Failed to open read end of client pipe. Error code %A" errno)
    else
    new UnixStream(readFd)

  let rec getWriting() =
    let writeFd = Syscall.``open``(writeFifoName, OpenFlags.O_WRONLY)
    if writeFd = -1 then
      let errno = Stdlib.GetLastError()
      if errno = Errno.EINTR then getWriting()
      elif errno = Errno.ENOENT then raise <| new UnknownRecipientException("npp: message recepient not found on remote target.", actorId)
      else failwith (sprintf "Failed to open write end of client pipe. Error code %A" errno)
    else
    new UnixStream(writeFd)

  //writes to posix compliant fifos are atomic only upto PIPE_BUF size writes
  //relying on this is complicated since there is no upper bound on message size
  //therefore we use a classic lock file to provide concurrency control on the fifo
  let flockName = tmp + pipeName + ".lock"
  let rec lockPipe () =    
    let flockFd = Syscall.``open``(flockName, OpenFlags.O_CREAT|||OpenFlags.O_EXCL|||OpenFlags.O_WRONLY, FilePermissions.S_IRUSR|||FilePermissions.S_IWUSR)
    if flockFd <> -1 then
      Syscall.close(flockFd) |> ignore
    elif flockFd = -1 && Stdlib.GetLastError() = Errno.EEXIST then
      Thread.Sleep 100
      lockPipe()
    else failwith (sprintf "Failed to lock pipe. Error code %A" <| Stdlib.GetLastError())
  let unlockPipe () = Syscall.unlink(flockName) |> ignore

  let mutable writing = Unchecked.defaultof<UnixStream>
  
  let post (R reply, msg: 'T) =
    async {
      try
        let data = serializer.Serialize<Request<'T>>(Message msg)
        do! writing.AsyncWriteBytes data

        use reading = getReading()
        let! replyData = reading.AsyncReadBytes()

        match serializer.Deserialize<Response> replyData with 
        | Acknowledge -> reply nothing
        | UnknownRecepient -> reply <| Exception (new UnknownRecipientException("npp: message recepient not found on remote target.", actorId))
        | Error e -> reply <| Exception (new DeliveryException("npp: message delivery failure.", actorId, e))
      with e -> reply <| Exception e
    }

  let poster = Actor.bind <| Behavior.stateless post

  override __.Poster = poster
  override __.Connect _ =
    lockPipe()
    writing <- getWriting()
  override __.Disconnect() =
    try
      Async.RunSynchronously <| writing.AsyncWriteBytes(serializer.Serialize<Request<'T>>(Disconnect))
      writing.Dispose()
    with _ -> ()
    writing <- Unchecked.defaultof<UnixStream>
    unlockPipe()
    
    
and internal PipeSenderWindows<'T> internal (pipeName: string, actorId: ActorId) =
  inherit PipeSender<'T>(pipeName, actorId)
  
  let serializer = Serialization.defaultSerializer
  let client = new NamedPipeClientStream(pipeName)

  let post (R reply, msg: 'T) =
    async {
      try
        if not client.IsConnected then reply <| Exception (new UnknownRecipientException("npp: message target is stopped or pipe is broken", actorId))
        else
    
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

  override __.Poster = poster
  override __.Connect(connectionTimeout: int) = client.Connect(connectionTimeout)
  override __.Disconnect() =
    Async.RunSynchronously <| client.AsyncWriteBytes(serializer.Serialize<Request<'T>>(Disconnect))
    client.Dispose()
