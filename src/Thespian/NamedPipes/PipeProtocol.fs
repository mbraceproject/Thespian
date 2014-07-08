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

[<Serializable>]
type PipeActorId internal (pipeName: string, actorName: string) =
  inherit ActorId(actorName)

  let idValue = sprintf "npp/%s/%s" pipeName actorName

  new (pipeName : string, actorRef : ActorRef) = new PipeActorId(pipeName, actorRef.Name)

  override __.ToString() = idValue

//
//  reply channels
//

type PipedReplyChannelReceiver<'R> (actorId: PipeActorId, timeout: int) =
  let chanId = Guid.NewGuid().ToString()
  let replyReceiver = new PipeReceiver<Reply<'R>>(pipeName = chanId, singularAccept = true)
  let await = Async.AwaitObservable (replyReceiver, timeout)

  member __.AwaitReply() = 
    async {
      try return! await
      with e ->
        return! Async.Raise <| CommunicationException(sprintf "PipeProtocol: error receiving reply from %O." actorId, e)
    }

  // pubishes a serialiable descriptor for this receiver
  member __.ReplyChannel = PipedReplyChannel<'R>(chanId, timeout)
  interface IDisposable with override __.Dispose() = replyReceiver.Stop()

and PipedReplyChannel<'R> internal (chanId: string, timeout: int) =
  let mutable timeout = timeout
        
  let reply v =
    try
      let client = PipeSender<Reply<'R>>(chanId)
      client.Post(v, connectionTimeout = timeout)
    with e -> raise <| CommunicationException("PipeProtocol: cannot reply.", e)

  let asyncReply v =
    try
      let client = PipeSender<Reply<'R>>(chanId)
      client.PostAsync(v, connectionTimeout = timeout)
    with e -> raise <| CommunicationException("PipeProtocol: cannot reply.", e)

  new (sI: SerializationInfo, _: StreamingContext) =
    let chanId = sI.GetString("chanId")
    let timeout = sI.GetInt32("timeout")
    new PipedReplyChannel<'R>(chanId, timeout)

  interface IReplyChannel<'R> with
    override __.Protocol = "npp"
    override __.Timeout with get () = timeout and set t = timeout <- t
    override __.ReplyUntyped(v: Reply<obj>) = reply (Reply.unbox v)
    override __.AsyncReplyUntyped(v: Reply<obj>) = asyncReply (Reply.unbox v)
    override __.Reply(v: Reply<'R>) = reply v
    override __.AsyncReply(v: Reply<'R>) = asyncReply v
    override __.WithTimeout t = new PipedReplyChannel<'R>(chanId, t) :> IReplyChannel<'R>

  interface ISerializable with
    override __.GetObjectData(sI: SerializationInfo, _: StreamingContext) =
      sI.AddValue("chanId", chanId)
      sI.AddValue("timeout", timeout)

//
//  the protocol
//

type PipeProtocolServer<'T>(pipeName: string, proc: Process, actorRef: ActorRef<'T>) =
  let actorId = new PipeActorId(pipeName, actorRef.Name)
  let server = new PipeReceiver<'T * IReplyChannel * bool>(pipeName)

  let forwarderErrors = new Event<exn>()
  let errorEvent = Event.merge forwarderErrors.Publish server.Errors

  let log = errorEvent |> Event.map(fun e -> LogLevel.Error, LogSource.Protocol "npp", e :> obj)

  
  let forwarder =
    let rec behaviour (self: Actor<'T * IReplyChannel * bool>) =
      let reportError e = try forwarderErrors.Trigger e with _ -> ()
      async {
        let! msg, (r : IReplyChannel), withReply = self.Receive ()

        let! response = actorRef.AsyncPost msg |> Async.Catch

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

  member __.Errors = errorEvent
  member __.ActorRef = actorRef
  member __.Stop() = 
    forwarder.Stop()
    server.Stop()

  interface IProtocolServer<'T> with
    override __.ProtocolName = "nnp"
    override __.ActorId = actorId :> ActorId
    override __.Client = new PipeProtocolClient<'T>(actorId.Name, pipeName, proc) :> IProtocolClient<'T>
    override __.Log = log
    override __.Start() = ()
    override __.Stop() = __.Stop()
    override __.Dispose() = __.Stop()


and PipeProtocolClient<'T>(actorName: string, pipeName: string, proc: Process) =
  let actorId = new PipeActorId(pipeName, actorName)
  let serializer = Serialization.defaultSerializer
  let sender = new PipeSender<'T * IReplyChannel * bool>(pipeName)

  let post (msg: 'T) =
    async {
      use rcr = new PipedReplyChannelReceiver<unit>(actorId, Timeout.Infinite)

      try do! sender.PostAsync <| (msg, rcr.ReplyChannel :> _, false)
      with e -> return! Async.Raise <| CommunicationException(sprintf "PipeProtocol: error communicating with %O." actorId, e)

      let! response = rcr.AwaitReply ()

      match response with
      | Value () -> return ()
      | Exception e -> return! Async.Raise e
    }

  let postWithReply' (msgB: IReplyChannel<'R> -> 'T, timeout: int) =
    async {
      use rcr = new PipedReplyChannelReceiver<'R>(actorId, timeout)
                
      let rc = rcr.ReplyChannel

      try do! sender.PostAsync((msgB rc, rc :> _, true), connectionTimeout = timeout)
      with e -> return! Async.Raise <| CommunicationException(sprintf "PipeProtocol: error communicating with %O" actorId, e)

      return! rcr.AwaitReply()
    }

  let postWithReply (msgF, timeout) =
    async {
      let! r = postWithReply'(msgF, timeout)
      match r with
      | Value v -> return v
      | Exception e -> return! Async.Raise e
    }

  let tryPostWithReply (msgF, timeout) =
    async {
      let! r = postWithReply'(msgF, timeout)
      match r with
      | Value v -> return Some v
      | Exception _ -> return None
    }
  
  interface IProtocolClient<'T> with
    override __.ProtocolName = "npp"
    override __.ActorId = actorId :> ActorId
    override __.Uri = String.Empty
    override __.Factory = Some (new PipeProtocolFactory(proc) :> IProtocolFactory)
    override __.Post(msg: 'T): unit = Async.RunSynchronously(post msg)
    override __.AsyncPost(msg: 'T): Async<unit> = post msg
    override __.PostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int): Async<'R> = postWithReply(msgF, timeout)
    override __.TryPostWithReply(msgF: IReplyChannel<'R> -> 'T, timeout: int): Async<'R option> = tryPostWithReply(msgF, timeout)


and PipeProtocolFactory(?proc: Process) =
  let proc = match proc with None -> Process.GetCurrentProcess () | Some p -> p

  let mkPipeName (actorName : string) = sprintf "pid-%d-actor-%s" proc.Id actorName
        
  member __.Pid = proc.Id

  interface IProtocolFactory with
    override __.ProtocolName = "npp"
    override __.CreateClientInstance<'T>(actorName: string) = new PipeProtocolClient<'T>(actorName, mkPipeName actorName, proc) :> IProtocolClient<'T>
    override __.CreateServerInstance<'T>(actorName: string, actorRef: ActorRef<'T>) = new PipeProtocolServer<'T>(mkPipeName actorName, proc, actorRef) :> IProtocolServer<'T>


module ActorRef =
  let ofProcess<'T> (proc : Process) (actorName : string) =
    let protoConf = new PipeProtocolFactory(proc) :> IProtocolFactory
    let proto = protoConf.CreateClientInstance<'T>(actorName)
    new ActorRef<'T>(actorName, [| proto |])
            
  let ofProcessId<'T> (pid : int) (name : string) = ofProcess<'T> (Process.GetProcessById pid) name

[<AutoOpen>]
module Protocol =
  let NPP = "npp"
  type Protocols with
    static member npp(?proc: Process) = new PipeProtocolFactory(?proc = proc) :> IProtocolFactory


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
