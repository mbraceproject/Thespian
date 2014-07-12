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
