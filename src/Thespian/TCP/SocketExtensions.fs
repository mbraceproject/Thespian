module Nessos.Thespian.Remote.SocketExtensions

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading

open Nessos.Thespian.AsyncExtensions

type TcpListener with
  member listener.AsyncAcceptTcpClient(): Async<TcpClient> =
    Async.FromBeginEnd(listener.BeginAcceptTcpClient, listener.EndAcceptTcpClient)

type TcpClient with
  member client.AsyncConnent(endPoint: IPEndPoint): Async<unit> =
    Async.FromBeginEnd(
      (fun (callback: System.AsyncCallback, state: obj) ->  client.BeginConnect(endPoint.Address, endPoint.Port, callback, state)), 
      client.EndConnect)

type Stream with
  //an AsyncRead with a timeout implemented with Begin/EndRead
  member self.TryAsyncRead(buffer: byte[], offset: int, size: int, timeout: int): Async<int option> =
    if timeout = 0 then async.Return None
    elif timeout = Timeout.Infinite then async { let! r = self.AsyncRead(buffer, offset, size) in return Some r }
    else Async.TryFromBeginEnd(buffer, offset, size, self.BeginRead, self.EndRead, timeout, fun () -> self.Dispose())

  member self.TryAsyncRead(count: int, timeout: int): Async<byte[] option> =
    let rec tryAsyncRead (buffer: byte[], bytesRead: int) =
      async {
        if count >= bytesRead then return Some()
        else
          let! r = self.TryAsyncRead(buffer, bytesRead, (count - bytesRead), timeout)
          match r with
          | Some bytesRead' -> return! tryAsyncRead (buffer, bytesRead + bytesRead')
          | None -> return None
      }
    async {
      let buffer = Array.zeroCreate count
      let! r = tryAsyncRead (buffer, 0)
      match r with
      | Some() -> return Some buffer
      | None -> return None
    }

  member self.TryAsyncWrite(buffer: byte[], timeout: int, ?offset: int, ?count: int, ?timeoutF: Stream -> unit): Async<unit option> =
    let offset = defaultArg offset 0
    let count = defaultArg count buffer.Length

    if timeout = 0 then async.Return None
    elif timeout = Timeout.Infinite then async { let! _ = self.AsyncWrite(buffer, offset, count) in return Some() }
    else Async.TryFromBeginEnd(buffer, offset, count, self.BeginWrite, self.EndWrite, timeout, fun () -> self.Dispose())

type Dns with
  static member AsyncGetHostAddresses(hostNameOrAddress: string): Async<IPAddress[]> =
    Async.FromBeginEnd(
      (fun (callback: System.AsyncCallback, state: obj) -> Dns.BeginGetHostAddresses(hostNameOrAddress, callback, state)),
      Dns.EndGetHostAddresses)
