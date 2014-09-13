open System
open System.Diagnostics
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks

type Atom<'T when 'T : not struct>(value : 'T) =
  let refCell = ref value

  let rec swap f = 
    let currentValue = !refCell
    let result = Interlocked.CompareExchange<'T>(refCell, f currentValue, currentValue)
    if obj.ReferenceEquals(result, currentValue) then ()
    else Thread.SpinWait 20; swap f

  let transact f =
    let result = ref Unchecked.defaultof<_>
    let f' t = let r,t' = f t in result := r ; t
    swap f' ; result.Value

  member self.Value with get() : 'T = !refCell
  member self.Swap (f : 'T -> 'T) : unit = swap f
  member self.Transact(f : 'T -> 'R * 'T) : 'R = transact f
  member self.Set (t : 'T) = swap (fun _ -> t)


[<RequireQualifiedAccess>]
module Atom =

  let atom value = new Atom<'T>(value)
  let swap (atom : Atom<'T>) f = atom.Swap f
  let transact (atom : Atom<'T>) f : 'R = atom.Transact f
  let set (atom : Atom<'T>) t = atom.Set t

type System.Threading.Tasks.Task with
  static member TimeoutAfter(t: Task<'T>, timeout: int): Task<'T option> =
    if t.IsCompleted || (timeout = Timeout.Infinite) then
      t.ContinueWith(new Func<Task<'T>, 'T option>(fun antecendant -> Some antecendant.Result), TaskContinuationOptions.NotOnCanceled|||TaskContinuationOptions.NotOnFaulted|||TaskContinuationOptions.ExecuteSynchronously)
    else
      let tcs = new TaskCompletionSource<'T option>()

      if timeout = 0 then
        tcs.TrySetResult(None) |> ignore
        tcs.Task
      else
        let timer = new Timer(new TimerCallback(fun state ->
          let tcs' = state :?> TaskCompletionSource<'T option>
          tcs'.TrySetResult(None) |> ignore), tcs, timeout, Timeout.Infinite)

        t.ContinueWith(new Action<Task<'T>, obj>(fun antecendant state ->
          let (timer', tcs'') = state :?> Timer * TaskCompletionSource<'T option>
          timer'.Dispose()
          match antecendant.Status with
          | TaskStatus.Faulted -> tcs''.TrySetException(antecendant.Exception) |> ignore
          | TaskStatus.Canceled -> tcs''.TrySetCanceled() |> ignore
          | TaskStatus.RanToCompletion -> tcs''.TrySetResult(Some antecendant.Result) |> ignore
          | _ -> failwith "unexpected task state"),
          (timer, tcs),
          CancellationToken.None,
          TaskContinuationOptions.ExecuteSynchronously,
          TaskScheduler.Default) |> ignore

        tcs.Task

type internal Latch() =
  let mutable switch = 0
  member __.Trigger() = Interlocked.CompareExchange(&switch, 1, 0) = 0        

type Microsoft.FSharp.Control.Async with
  // untyped awaitTask
  static member AwaitTask1 (t : Task) = t.ContinueWith ignore |> Async.AwaitTask

  static member AwaitTask2(t: Task<'T>, timeout: int) =
    Async.AwaitTask <| Task.TimeoutAfter(t, timeout)
  
  static member AwaitTask3 (t : Task<'T>, timeout : int) =
    async {
      let! ct = Async.CancellationToken
      use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
      use timer = Task.Delay (timeout, cts.Token)
      let tcs = new TaskCompletionSource<bool>()
      use _ = ct.Register(new Action<obj>(fun s -> (s :?> TaskCompletionSource<bool>).TrySetResult(true) |> ignore), tcs)
      try
        let! completed = Async.AwaitTask <| Task.WhenAny(t, tcs.Task, timer)
        if completed = (t :> Task) then
          let! result = Async.AwaitTask t
          return Some result
        else if completed = (tcs.Task :> Task) then
         return raise (new OperationCanceledException(ct))
        else return None
      finally cts.Cancel()
    }

  static member ConditionalCancel (condition: Async<bool>) (computation: Async<'T>): Async<'T option> =
    async {
      let! ct = Async.CancellationToken
      return! Async.FromContinuations(fun (success, error, _) ->
        let ctsTimeout = System.Threading.CancellationTokenSource.CreateLinkedTokenSource([| ct |])
        let ctsComp = System.Threading.CancellationTokenSource.CreateLinkedTokenSource([| ct |])
        let latch = new Latch()
        let pSuccess v = if latch.Trigger() then success v
        let pError e = if latch.Trigger() then error e
        let k = async {
            let! r = Async.Catch computation
            ctsTimeout.Cancel()
            match r with
            | Choice1Of2 v -> pSuccess (Some v)
            | Choice2Of2 e -> pError e
        }
        let t = async {
            let! r = Async.Catch condition
            match r with
            | Choice1Of2 true -> 
                ctsComp.Cancel()
                pSuccess None
            | Choice1Of2 false -> ()
            | Choice2Of2 e ->
                ctsComp.Cancel()
                pError e
        }
        Async.Start(k, ctsComp.Token)
        Async.Start(t, ctsTimeout.Token)
      )
    }

  static member WithTimeout (timeout: int) (computation: Async<'T>): Async<'T option> =
    let cancelCondition =
      async {
        do! Async.Sleep timeout
        return true
      }
    Async.ConditionalCancel cancelCondition computation


type AsyncResultCell<'T>() =
  let completionSource = new TaskCompletionSource<'T>()

  let t = completionSource.Task

  member c.RegisterResult(result: 'T) = completionSource.SetResult(result)
  member c.AsyncWaitResult(millisecondsTimeout: int): Async<'T option> =
        Async.AwaitTask2(completionSource.Task, millisecondsTimeout)

  // use default AwaitTask when no timeout overload is given
  member c.AsyncWaitResult(): Async<'T> = async {
    let! r = Async.AwaitTask2(completionSource.Task, Timeout.Infinite)
    return Option.get r
  }


type AtomCellResultRegistry() =
  let registry = Atom.atom Map.empty<string, AsyncResultCell<int>>

  member __.RegisterAndWaitForResponse(key: string, timeout: int) =
    let resultCell = new AsyncResultCell<int>()
    Atom.swap registry (Map.add key resultCell)

    let waitForResult =
      async {
        let! result = resultCell.AsyncWaitResult(timeout)
        Atom.swap registry (Map.remove key)
        return result
      }

    waitForResult

  member __.SetResult(key: string, value: int) =
    let resultCell = registry.Value.[key]
    resultCell.RegisterResult value

type DictionaryCellResultRegistry() =
  let registry = new ConcurrentDictionary<string, AsyncResultCell<int>>()

  member __.RegisterAndWaitForResponse(key: string, timeout: int) =
    let resultCell = new AsyncResultCell<int>()
    registry.TryAdd(key, resultCell) |> ignore
    let waitForResult =
      async {
        let! result = resultCell.AsyncWaitResult(timeout)
        let _, _ = registry.TryRemove(key)
        return result
      }

    waitForResult

  member __.SetResult(key: string, value: int) =
    let isValid, resultCell = registry.TryGetValue(key)
    if isValid then resultCell.RegisterResult value


type DictionaryContinuationsRegistry() =
  let registry = new ConcurrentDictionary<string, int -> unit>()
  let unconsumedResults = new ConcurrentDictionary<string, int>()

  member __.RegisterAndWaitForResponse(key: string, timeout: int) =
    Async.FromContinuations(fun (k, _, _) ->
      registry.TryAdd(key, k) |> ignore
      let isValid, i = unconsumedResults.TryRemove(key)
      if isValid then
        let isValid', k' = registry.TryRemove(key)
        if isValid' then k' i
    )

  member __.SetResult(key: string, value: int) =
    unconsumedResults.TryAdd(key, value) |> ignore
    let isValid, k = registry.TryRemove(key)
    if isValid then k value

type DictionaryContinuationsTimeoutCtsRegistry() =
  let registry = new ConcurrentDictionary<string, int -> unit>()
  let unconsumedResults = new ConcurrentDictionary<string, int>()

  member __.RegisterAndWaitForResponse(key: string, timeout: int) =
    Async.FromContinuations(fun (k, e, _) ->
      let cts = new CancellationTokenSource()
      registry.TryAdd(key, fun r -> cts.Cancel(); k r) |> ignore
      let isValid, i = unconsumedResults.TryRemove(key)
      if isValid then
        let isValid', k' = registry.TryRemove(key)
        if isValid' then k' i
      else Task.Delay(timeout, cts.Token).ContinueWith(fun _ -> let isValid'', _ = registry.TryRemove(key) in if isValid'' && not cts.IsCancellationRequested then e(new TimeoutException())) |> ignore
    )

  member __.SetResult(key: string, value: int) =
    unconsumedResults.TryAdd(key, value) |> ignore
    let isValid, k = registry.TryRemove(key)
    if isValid then k value


type DictionaryContinuationsTimeoutRegistry() =
  let registry = new ConcurrentDictionary<string, int -> unit>()
  let unconsumedResults = new ConcurrentDictionary<string, int>()

  member __.RegisterAndWaitForResponse(key: string, timeout: int) =
    Async.FromContinuations(fun (k, e, _) ->
      registry.TryAdd(key, k) |> ignore
      let isValid, i = unconsumedResults.TryRemove(key)
      if isValid then
        let isValid', k' = registry.TryRemove(key)
        if isValid' then k' i
      else Task.Delay(timeout).ContinueWith(fun _ -> let isValid'', _ = registry.TryRemove(key) in if isValid'' then e(new TimeoutException())) |> ignore
    )

  member __.SetResult(key: string, value: int) =
    unconsumedResults.TryAdd(key, value) |> ignore
    let isValid, k = registry.TryRemove(key)
    if isValid then k value


type DictionaryMailboxRegistry() =
  let registry = new ConcurrentDictionary<string, MailboxProcessor<int>>()

  member __.RegisterAndWaitForResponse(key: string, timeout: int) =
    let mailbox = new MailboxProcessor<int>(fun _ -> async.Return())
    mailbox.Start()
    registry.TryAdd(key, mailbox) |> ignore
    async {
      use _ = mailbox
      return! mailbox.TryReceive(timeout)
    }

  member __.SetResult(key: string, value: int) =
    let isValid, mailbox = registry.TryRemove(key)
    if isValid then mailbox.Post value
    else raise <| new KeyNotFoundException()

#time

let results = 1000000
let ids = [| for _ in 1..results -> Guid.NewGuid().ToString() |]

//type ResultRegistry = AtomCellResultRegistry
type ResultRegistry = DictionaryCellResultRegistry
//type ResultRegistry = DictionaryContinuationsRegistry
//type ResultRegistry = DictionaryContinuationsTimeoutCtsRegistry
//type ResultRegistry = DictionaryContinuationsTimeoutRegistry
//type ResultRegistry = DictionaryMailboxRegistry

let registry = new ResultRegistry()

let waits = ids |> Array.Parallel.map (fun id -> registry.RegisterAndWaitForResponse(id, 5000))

let top =
  let waitForResults =
    async {
      let! r = Async.Parallel waits
      assert (r.Length = waits.Length)
      return r
    }
  let setResults =
    async {
//      do! Async.Sleep 100
      do ids |> Array.Parallel.iteri (fun i key -> registry.SetResult(key, i))
      return Array.empty
    }
  Async.Parallel [waitForResults; setResults]
  |> Async.RunSynchronously


let oneMilCts =
  [| for _ in 1..1000000 -> new CancellationTokenSource() |]
  

let testFromCont =
  Async.FromContinuations(fun (k, _, _) -> k 42; k 0)

let test =
  async {
    let! i = testFromCont
    printfn "%d" i
    return i
  }

Async.RunSynchronously test


let withTimeoutThroughTask (computation: Async<'T>) (timeout: int) =
  async {
    let t = Async.StartAsTask computation
    return! Async.AwaitTask <| Task.TimeoutAfter(t, timeout)
  }

let withTimeoutStartChild (computation: Async<'T>) (timeout: int) =
  async {
    let! c = Async.StartChild(computation, timeout)
    try let! r = c in return Some r with :? TimeoutException -> return None
  }

//let withTimeout = withTimeoutStartChild
//let withTimeout (computation: Async<'T>) (timeout: int) = Async.WithTimeout timeout computation
let withTimeout = withTimeoutThroughTask

let size = 10
let timeout = 5000
let work = 1000000

let worker i =
  async {
//    for _ in 1..work do ()
    do! Async.Sleep 100
    return i
  }

let topTimeoutSequential =
  async {
    for i in 1..size do
      do! Async.Ignore <| withTimeout (worker i) timeout
  }
  |> Async.RunSynchronously


let topParallel =
  [| for i in 1..work -> withTimeout (worker i) timeout |]
  |> Async.Parallel
  |> Async.RunSynchronously


let mutable counter = 0

let incParallel =
  [| for i in 1..work -> async { return Interlocked.Increment(&counter) } |]
  |> Async.Parallel
  |> Async.RunSynchronously


open System
open System.Net
open System.Net.Sockets

module SocketExtensions =
  open System
  open System.Net
  open System.Net.Sockets
  open System.Threading

  type TcpListener with
    member listener.AsyncAcceptTcpClient(): Async<TcpClient> =
      Async.FromBeginEnd(listener.BeginAcceptTcpClient, listener.EndAcceptTcpClient)

  type TcpClient with
    member client.AsyncConnent(endPoint: IPEndPoint): Async<unit> =
      Async.FromBeginEnd(
        (fun (callback: System.AsyncCallback, state: obj) ->  client.BeginConnect(endPoint.Address, endPoint.Port, callback, state)), 
        client.EndConnect)

  type NetworkStream with
    member self.TryAsyncRead(buffer: byte[], offset: int, size: int, timeout: int) =
      if timeout = 0 then async.Return None
      elif timeout = Timeout.Infinite then async { let! r = self.AsyncRead(buffer, offset, size) in return Some r }
      else
        let timer = ref Unchecked.defaultof<Timer>
        let latch = ref 0
        Async.FromBeginEnd(buffer, offset, size,
          (fun (buffer', offset', size', callback, state) ->
             let iar = self.BeginRead(buffer', offset', size', callback, state)
             if not iar.IsCompleted then
               timer := new Timer(
                 (fun _ -> if Interlocked.CompareExchange(latch, 1, 0) = 0 then timer.Value.Dispose(); timer := Unchecked.defaultof<Timer>; self.Dispose()),
                 null, timeout, Timeout.Infinite)
             iar),
          (fun iar ->
             if timer.Value <> Unchecked.defaultof<Timer> then timer.Value.Dispose()
             if Interlocked.CompareExchange(latch, 1, 0) = 0 then
               let r = self.EndRead(iar) in Some r
             else try let r = self.EndRead(iar) in Some r with :? ObjectDisposedException -> None))

      member self.TryAsyncRead(count: int, timeout: int) =
        async {
          let buffer = Array.zeroCreate count
          return! self.TryAsyncRead(buffer, 0, count, timeout)
        }

open SocketExtensions

let server = new TcpListener(IPAddress.Any, 4242)
server.Start()

let buffer: byte[] = Array.zeroCreate 1
let srv =
  async {
    try
      use! c = server.AsyncAcceptTcpClient()
      printfn "connected"
      use s = c.GetStream()
//      let! r = Task.TimeoutAfter(s.ReadAsync(buffer, 0, 1), 1000) |> Async.AwaitTask
      let! r = s.TryAsyncRead(buffer, 0, 1, 10000)
      printfn "Read %A" r
    with e -> printfn "Exception %A" e
  }

Async.Start srv

let c = new TcpClient()
c.Connect(new IPEndPoint(IPAddress.Loopback, 4242))
let s = c.GetStream()
s.AsyncWrite([| 0uy |]) |> Async.RunSynchronously

server.Stop()


type Foo() =
  let str = "somestring"
  let num = 4242
  override __.ToString() = sprintf "%s:%d" str num

type Bar() =
  let str1 = "something"
  let foo = new Foo()
  let str2 = "else"
  override __.ToString() = sprintf "%s/%O/%s" str1 foo str2

let r = 
  [ for _ in 1..10000 -> async {
    let bar = new Bar()
    do! Async.Sleep 100
    return bar.ToString() } ]
  |> Async.Parallel
  |> Async.RunSynchronously

open System.Threading.Tasks

let r2 = [ for _ in 1..1000000 -> Task.Factory.StartNew(fun () -> let bar = new Bar() in bar.ToString()) ]
