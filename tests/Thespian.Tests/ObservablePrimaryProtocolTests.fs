namespace Nessos.Thespian.Tests

open NUnit.Framework
open Nessos.Thespian

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
