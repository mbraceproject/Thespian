namespace Nessos.Thespian.Remote.PipeProtocol

open System
open System.Collections.Concurrent
open System.IO
open System.IO.Pipes
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization

//when in unix, use unixpipes instead of .net named pipes
open Mono.Unix
open Mono.Unix.Native
    
open Nessos.Thespian
open Nessos.Thespian.AsyncExtensions
open Nessos.Thespian.Serialization
open Nessos.Thespian.Utils


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


[<Serializable>]
type PipeActorId internal (pipeName: string, actorName: string) =
  inherit ActorId(actorName)

  let idValue = sprintf "npp/%s/%s" pipeName actorName

  new (pipeName : string, actorRef : ActorRef) = new PipeActorId(pipeName, actorRef.Name)

  override __.ToString() = idValue

//
//  reply channels
//
and [<AbstractClass>] PipedReplyChannelReceiver() =
  abstract AwaitReplyUntyped: int -> Async<Reply<obj> option>

and PipedReplyChannelReceiver<'R> (actorId: PipeActorId) =
  inherit PipedReplyChannelReceiver()
  let chanId = Guid.NewGuid().ToString()
  let tcs = new TaskCompletionSource<Reply<'R>>()
  let processReply (reply: Reply<'R>) = tcs.TrySetResult(reply) |> ignore
  let replyReceiver = PipeReceiver<Reply<'R>>.Create(pipeName = chanId, processMessage = processReply, singleAccept = true)

  do replyReceiver.Start()

  member __.AwaitReply(timeout: int) = Async.AwaitTask(tcs.Task, timeout)
  override self.AwaitReplyUntyped(timeout: int) =
    async {
      let! r = self.AwaitReply(timeout)
      match r with
      | Some(Value v) -> return Some(Value <| box v)
      | Some(Exception e) -> return Some(Exception e)
      | None -> return None
    }

  member self.ReplyChannel = new PipedReplyChannel<'R>(actorId, chanId, Some self)

//  interface IDisposable with override __.Dispose() = replyReceiver.Stop()

and PipedReplyChannel(receiver: PipedReplyChannelReceiver option) =
  member __.Receiver = receiver

and PipedReplyChannel<'R> internal (actorId: PipeActorId, chanId: string, receiver: PipedReplyChannelReceiver<'R> option, ?timeout: int) =
  inherit PipedReplyChannel(receiver |> Option.map (fun r -> r :> PipedReplyChannelReceiver))
  let mutable timeout = defaultArg timeout Default.ReplyReceiveTimeout
        
  let reply (v: Reply<'R>) =
    try
      use client = PipeSender<Reply<'R>>.GetPipeSender(chanId, actorId)
      client.Post(v)
    with e -> raise <| CommunicationException("PipeProtocol: cannot reply.", e)

  let asyncReply (v: Reply<'R>) =
    async {
      try
        use client = PipeSender<Reply<'R>>.GetPipeSender(chanId, actorId)
        return! client.PostAsync(v)
      with e -> return! Async.Raise <| CommunicationException("PipeProtocol: cannot reply.", e)
    }

  new (sI: SerializationInfo, _: StreamingContext) =
    let actorId = sI.GetValue("actorId", typeof<PipeActorId>) :?> PipeActorId
    let chanId = sI.GetString("chanId")
    let timeout = sI.GetInt32("timeout")
    new PipedReplyChannel<'R>(actorId, chanId, None, timeout)

  interface IReplyChannel<'R> with
    override __.Protocol = ProtocolName
    override __.Timeout with get () = timeout and set t = timeout <- t
    override __.ReplyUntyped(v: Reply<obj>) = reply (Reply.unbox v)
    override __.AsyncReplyUntyped(v: Reply<obj>) = asyncReply (Reply.unbox v)
    override __.Reply(v: Reply<'R>) = reply v
    override __.AsyncReply(v: Reply<'R>) = asyncReply v
    override self.WithTimeout t = let self' = self :> IReplyChannel<'R> in self'.Timeout <- t; self'

  interface ISerializable with
    override __.GetObjectData(sI: SerializationInfo, _: StreamingContext) =
      sI.AddValue("actorId", actorId)
      sI.AddValue("chanId", chanId)
      sI.AddValue("timeout", timeout)

//
//  the protocol
//

and PipeProtocolServer<'T>(pipeName: string, processId: int, actorRef: ActorRef<'T>) =
  let actorId = new PipeActorId(pipeName, actorRef.Name)
  let server = PipeReceiver<'T>.Create(pipeName, actorRef.Post)
  let errorEvent = server.Errors
  let log = errorEvent |> Event.map(fun e -> LogLevel.Error, LogSource.Protocol ProtocolName, e :> obj)

  member __.Errors = errorEvent
  member __.ActorRef = actorRef
  member __.Start() = server.Start()
  member __.Stop() = server.Stop()

  interface IProtocolServer<'T> with
    override __.ProtocolName = "nnp"
    override __.ActorId = actorId :> ActorId
    override __.Client = new PipeProtocolClient<'T>(actorId.Name, pipeName, processId) :> IProtocolClient<'T>
    override __.Log = log
    override __.Start() = __.Start()
    override __.Stop() = __.Stop()
    override __.Dispose() = __.Stop()


and PipeProtocolClient<'T>(actorName: string, pipeName: string, processId: int) =
  let actorId = new PipeActorId(pipeName, actorName)
  let serializer = Serialization.defaultSerializer

  let post (msg: 'T) =
    async {
      try
        use sender = PipeSender<'T>.GetPipeSender(pipeName, actorId)
        do! sender.PostAsync msg
      with :? CommunicationException as e -> return! Async.Raise e
          | e -> return! Async.Raise <| CommunicationException(sprintf "PipeProtocol: error communicating with %O." actorId, e)
    }

  member private __.TryPostWithReplyInner(msgF: IReplyChannel<'R> -> 'T, timeout: int) =
    async {
      let rcr = new PipedReplyChannelReceiver<'R>(actorId)
      let rc = rcr.ReplyChannel :> IReplyChannel<'R>
      let initTimeout = rc.Timeout
      let msg = msgF (new ReplyChannelProxy<'R>(rc))
      let timeout' = if initTimeout <> rc.Timeout then rc.Timeout else timeout

      try
        use sender = PipeSender<'T>.GetPipeSender(pipeName, actorId)
        do! sender.PostAsync msg
        return! rcr.AwaitReply timeout'
      with :? CommunicationException as e -> return! Async.Raise e
          | e -> return! Async.Raise <| CommunicationException(sprintf "PipeProtocol: error communicating with %O" actorId, e)
    }

  member private self.TryPostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int) =
    async {
      let! r = self.TryPostWithReplyInner(msgF, timeout)
      match r with
      | Some(Value v) -> return Some v
      | Some(Exception e) -> return! Async.Raise <| new MessageHandlingException("An exception occurred while on the remote recepient while processing the message.", actorId, e)
      | None -> return None
    }

  member private self.PostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int) =
    async {
      let! r = self.TryPostWithReply(msgF, timeout)
      match r with
      | Some v -> return v
      | None -> return! Async.Raise <| new TimeoutException("Timeout occurred while waiting for reply.")
    }
  
  interface IProtocolClient<'T> with
    override __.ProtocolName = ProtocolName
    override __.ActorId = actorId :> ActorId
    override __.Uri = String.Empty
    override __.Factory = Some (new PipeProtocolFactory(processId) :> IProtocolFactory)
    override __.Post(msg: 'T): unit = Async.RunSynchronously(post msg)
    override __.AsyncPost(msg: 'T): Async<unit> = post msg
    override __.PostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int): Async<'R> = __.PostWithReply(msgF, timeout)
    override __.TryPostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int): Async<'R option> = __.TryPostWithReply(msgF, timeout)


and PipeProtocolFactory(?processId: int) =
  let processId = defaultArg processId <| Process.GetCurrentProcess().Id

  let mkPipeName (actorName : string) = sprintf "pid-%d-actor-%s" processId actorName
        
  member __.Pid = processId

  interface IProtocolFactory with
    override __.ProtocolName = ProtocolName
    override __.CreateClientInstance<'T>(actorName: string) = new PipeProtocolClient<'T>(actorName, mkPipeName actorName, processId) :> IProtocolClient<'T>
    override __.CreateServerInstance<'T>(actorName: string, actorRef: ActorRef<'T>) = new PipeProtocolServer<'T>(mkPipeName actorName, processId, actorRef) :> IProtocolServer<'T>


and private Request<'T> =
  | Message of 'T
  | Disconnect

and private Response =
  | Acknowledge
  | UnknownRecepient
  | Error of exn

and [<AbstractClass>] PipeReceiver<'T>(pipeName: string) =
  static let onUnix =
    let p = (int)System.Environment.OSVersion.Platform
    p = 4 || p = 6 || p = 128
    
  member __.PipeName = pipeName

  abstract Errors: IEvent<exn>
  abstract Start: unit -> unit
  abstract Stop: unit -> unit

  interface IDisposable with override __.Dispose() = __.Stop()

  static member Create(pipeName: string, processMessage: 'T -> unit, ?singleAccept: bool): PipeReceiver<'T> =
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

and [<AbstractClass>] internal PipeSender<'T> internal (pipeName: string, actorId: PipeActorId) =
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

  let serializationContext() =
    new MessageSerializationContext(serializer,
      {
        new IReplyChannelFactory with 
          override __.Protocol = ProtocolName
          override __.Filter(rc: IReplyChannel<'U>) = rc.Protocol <> ProtocolName
          override __.Create<'R>() =
            let receiver = new PipedReplyChannelReceiver<'R>(actorId)
            new ReplyChannelProxy<'R>(receiver.ReplyChannel)
      })

  let handleForeignReplyChannel (foreignRc: IReplyChannel, nativeRc: IReplyChannel) =
    let nativeRcImpl = nativeRc :?> PipedReplyChannel
    async {
      let! response = nativeRcImpl.Receiver.Value.AwaitReplyUntyped(foreignRc.Timeout)
      match response with
      | Some reply ->
        try do! foreignRc.AsyncReplyUntyped reply with _ -> () //TODO! log this
      | None -> ()
    }

  let setupForeignReplyChannelHandler (context: MessageSerializationContext) =
    if context.ReplyChannelOverrides.Length = 0 then async.Zero()
    else
    context.ReplyChannelOverrides
    |> List.map handleForeignReplyChannel
    |> Async.Parallel
    |> Async.Ignore

  member __.SerializeDataAndHandleForeignRcs(msg: 'T) =
    let context = serializationContext()
    let data = serializer.Serialize<Request<'T>>(Message msg, context.GetStreamingContext())

    setupForeignReplyChannelHandler context |> Async.Start
    data

  abstract Poster: Actor<IReplyChannel<unit> * 'T>
  abstract Connect: int -> unit
  abstract Disconnect: unit -> unit

  member self.PostAsync(msg: 'T): Async<unit> =
    async {
      try return! !self.Poster <!- fun ch -> ch.WithTimeout(Timeout.Infinite), msg
      with :? MessageHandlingException as e -> return! Async.Raise e.InnerException
    }

  member __.Post(msg: 'T): unit = __.PostAsync(msg) |> Async.RunSynchronously

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

  static member GetPipeSender(pipeName: string, actorId: PipeActorId): PipeSender<'T> =
    let sender = senders.GetOrAdd(pipeName, fun _ -> if onUnix then new PipeSenderUnix<'T>(pipeName, actorId) :> PipeSender<'T> else new PipeSenderWindows<'T>(pipeName, actorId) :> PipeSender<'T>)
    if sender.Acquire() then sender
    else Thread.SpinWait(20); PipeSender<'T>.GetPipeSender(pipeName, actorId)


and internal PipeSenderUnix<'T> internal (pipeName: string, actorId: PipeActorId) as self =
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
        let data = self.SerializeDataAndHandleForeignRcs msg
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
    
    
and internal PipeSenderWindows<'T> internal (pipeName: string, actorId: PipeActorId) as self =
  inherit PipeSender<'T>(pipeName, actorId)
  
  let serializer = Serialization.defaultSerializer
  let client = new NamedPipeClientStream(pipeName)

  let post (R reply, msg: 'T) =
    async {
      try
        if not client.IsConnected then reply <| Exception (new UnknownRecipientException("npp: message target is stopped or pipe is broken", actorId))
        else

        let data = self.SerializeDataAndHandleForeignRcs msg
        
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



module ActorRef =
  let ofProcess<'T> (proc : Process) (actorName : string) =
    let protoConf = new PipeProtocolFactory(proc.Id) :> IProtocolFactory
    let proto = protoConf.CreateClientInstance<'T>(actorName)
    new ActorRef<'T>(actorName, [| proto |])
            
  let ofProcessId<'T> (pid : int) (name : string) = ofProcess<'T> (Process.GetProcessById pid) name

[<AutoOpen>]
module Protocol =
  let NPP = ProtocolName
  type Protocols with
    static member npp(?processId: int) = new PipeProtocolFactory(?processId = processId) :> IProtocolFactory


// type PipeProtocol<'T> private (config : PipeProtocolConfig, pipeName : string, 
//                                         serializer : IMessageSerializer, actorName : string,
//                                         actorUUID : ActorUUID, server : PipeProtocolServer<'T> option) =

//         static let mkPipeName (config : PipeProtocolConfig) actorName =
//             sprintf "pid-%d-actor-%s" config.Pid actorName

//         let actorId = PipeActorId(pipeName, actorUUID, actorName)

//         // whatever ..
//         let eventLog = new Event<Log> ()
//         let events =
//             match server with
//             | Some server -> 
//                 server.Errors 
//                 |> Event.map(fun e -> LogLevel.Error, LogSource.Protocol "npp", e :> obj)
//                 |> Event.merge eventLog.Publish
//             | None -> eventLog.Publish

//         let sender = new PipeSender<'T * IReplyChannel * bool>(pipeName, serializer)

//         let post (msg : 'T) =
//             async {
//                 use rcr = new PipedReplyChannelReceiver<unit>(actorId, serializer, Timeout.Infinite)

//                 try
//                     do! sender.PostAsync <| (msg, rcr.ReplyChannel :> _, false)
//                 with e ->
//                     return! Async.Raise <|
//                         CommunicationException(sprintf "PipeProtocol: error communicating with %O." actorId, e)

//                 let! response = rcr.AwaitReply ()

//                 match response with
//                 | Value () -> return ()
//                 | Exception e -> return! Async.Raise e
//             }

//         let postWithReply (msgB : IReplyChannel<'R> -> 'T, timeout) =
//             async {
//                 use rcr = new PipedReplyChannelReceiver<'R>(actorId, serializer, timeout)
                
//                 let rc = rcr.ReplyChannel

//                 try
//                     do! sender.PostAsync((msgB rc, rc :> _, true), connectionTimeout = timeout)
//                 with e ->
//                     return! Async.Raise <|
//                         CommunicationException(sprintf "PipeProtocol: error communicating with %O" actorId, e)

//                 return! rcr.AwaitReply ()
//             }

//         new (config : PipeProtocolConfig, actorRef : ActorRef<'T>) =
//             let pipeName = mkPipeName config actorRef.Name
//             let server = new PipeProtocolServer<'T>(pipeName, config.Serializer, actorRef)
//             new PipeProtocol<'T>(config, pipeName, config.Serializer, actorRef.Name, actorRef.UUId, Some server) 

//         new (config : PipeProtocolConfig, actorUUID : ActorUUID, actorName : string) =
//             let pipeName = mkPipeName config actorName
//             new PipeProtocol<'T>(config, pipeName, config.Serializer, actorName, actorUUID, None)

//         interface IActorProtocol<'T> with
//             member __.ActorId = actorId :> _
//             member __.ActorUUId = actorUUID
//             member __.ActorName = actorName
//             member __.MessageType = typeof<'T>
//             member __.ProtocolName = "npp"
//             member __.Log = events
//             member __.Configuration = Some (config :> IProtocolConfiguration)
//             member __.Start () = ()
//             member __.Stop () = server |> Option.iter (fun s -> s.Stop())
//             member __.Post msg = Async.RunSynchronously(post msg)
//             member __.PostAsync msg = post msg
//             member __.PostWithReply<'R> (msgB, timeout) : Async<'R> = 
//                 async {
//                     let! r = postWithReply(msgB, timeout)
//                     match r with
//                     | Value v -> return v
//                     | Exception e -> return! Async.Raise e
//                 }
//             member __.TryPostWithReply<'R> (msgB, timeout) : Async<'R option> =
//                 async {
//                     let! r = postWithReply(msgB, timeout)
//                     match r with
//                     | Value v -> return Some v
//                     | Exception _ -> return None
//                 }
//             member __.PostWithReply<'R> msg : Async<'R> =
//                 async {
//                     let! r = postWithReply (msg, Timeout.Infinite)
//                     match r with
//                     | Value v -> return v
//                     | Exception e -> return! Async.Raise e
//                 }


//     and PipeProtocolConfig(?proc : Process, ?serializer : IMessageSerializer) =
//         let isServer = proc.IsNone
//         let proc = match proc with None -> Process.GetCurrentProcess () | Some p -> p
//         let serializer = 
//             match serializer with
//             | None -> SerializerRegistry.GetDefaultSerializer()
//             | Some s -> s
        
//         let mkPipeName (actorName : string) =
//             sprintf "pid-%d-actor-%s" proc.Id actorName

//         let compareTo (y : obj) =
//             match y with
//             | :? PipeProtocolConfig as y -> compare proc.Id y.Pid
//             | :? IProtocolConfiguration as y -> compare "npp" y.ProtocolName
//             | _ -> invalidArg "y" "invalid comparand"
        
//         member __.Pid = proc.Id
//         member __.Serializer = serializer

//         member s.GetClientInstance<'T> (uuid : ActorUUID, name : string) =
//             new PipeProtocol<'T>(s, uuid, name)

//         interface IProtocolConfiguration with
//             member c.ProtocolName = "npp"
//             member c.Serializer = None
//             member c.CreateProtocolInstances<'T> (actorRef : ActorRef<'T>) =
//                 if isServer then
//                     [| new PipeProtocol<'T>(c, actorRef) :> IActorProtocol<'T> |]
//                 else
//                     invalidOp "pipe protocol configuration is not server mode."
//             member c.CreateProtocolInstances<'T> (uuid : ActorUUID, name : string) = 
//                 [| new PipeProtocol<'T>(c, uuid, name) :> IActorProtocol<'T> |]
// //            member c.TryCreateProtocolInstances (uuid: ActorUUID, name : string) =
// //                try Some (new PipeProtocol<'T>(c, uuid, name) :> IActorProtocol<'T>)
// //                with _ -> None
            
//             member x.CompareTo (y : obj) = compareTo y
//             member x.CompareTo (y : IProtocolConfiguration) = compareTo y


//     type PipeProtocol = PipeProtocolConfig
