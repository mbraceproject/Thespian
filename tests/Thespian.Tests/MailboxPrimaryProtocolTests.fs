namespace Nessos.Thespian.Tests

open NUnit.Framework
open Nessos.Thespian

[<TestFixture>]
type ``Mailbox Primary Protocol Tests``() = 
    inherit PrimaryProtocolTests(new MailboxProtocol.MailboxPrimaryProtocolFactory() :> IPrimaryProtocolFactory)
