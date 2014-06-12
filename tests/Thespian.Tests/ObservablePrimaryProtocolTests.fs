namespace Nessos.Thespian.Tests

open System
open NUnit.Framework
open FsUnit
open Nessos.Thespian
open Nessos.Thespian.AsyncExtensions

module ObservableTestUtils =

  let receiver = new Receiver<obj>("testReceiver", [| new Mailbox.MailboxProtocolServer<_>("testReceiver") :> IProtocolServer<_> |])

  type ProtocolFactory() =
    interface IPrimaryProtocolFactory with
      override __.Create<'T>(actorName: string) =
        let observable = receiver.ReceiveEvent |> Observable.choose (function :? 'T as msg -> Some msg | _ -> None)
        new Observable.ObservableProtocolServer<'T>(actorName, observable) :> IPrimaryProtocolServer<'T>

  do receiver.Start()

[<TestFixture>]
type ``Observable Primary Protocol Tests``() =
  inherit PrimaryProtocolTests(new ObservableTestUtils.ProtocolFactory() :> IPrimaryProtocolFactory)

  [<Test>]
  [<ExpectedException(typeof<ActorInactiveException>)>]
  member __.``Receiver not started``() =
    use receiver = Receiver.create()
    !receiver <-- 42

  [<Test>]
  member __.``Post to receiver``() =
    use receiver = Receiver.create() |> Receiver.start

    let awaitResult = receiver |> Receiver.toObservable |> Async.AwaitObservable

    !receiver <-- 42
    awaitResult |> Async.RunSynchronously |> should equal 42

  [<Test>]
  member __.``Receiver start/stop``() =
    use receiver = Receiver.create() |> Receiver.start
    !receiver <-- 42

    receiver.Stop()
    TestDelegate(fun () -> !receiver <-- 42) |> should throw typeof<ActorInactiveException>

    receiver.Start()
    !receiver <-- 42

  [<Test>]
  [<ExpectedException(typeof<ArgumentException>)>]
  member __.``Receiver invalid name``() =
    new Receiver<int>("foo/bar") |> ignore

  [<Test>]
  [<ExpectedException(typeof<ArgumentException>)>]
  member __.``Receiver invalid rename``() =
    Receiver.create() |> Receiver.rename "foo/bar" |> ignore
