namespace Nessos.Thespian.Tests

open Nessos.Thespian

type ``Mailbox Primary Protocol Tests``() =
  inherit BaseTests(new MailboxPrimaryProtocolFactory() :> IPrimaryProtocolFactory)
