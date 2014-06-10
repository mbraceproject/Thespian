namespace Nessos.Thespian.Tests

open NUnit.Framework
open Nessos.Thespian

[<TestFixture>]
type ``Mailbox Primary Protocol Tests``() =
  inherit BaseTests(new MailboxPrimaryProtocolFactory() :> IPrimaryProtocolFactory)
