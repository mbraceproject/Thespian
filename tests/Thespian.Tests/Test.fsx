#r "../../bin/FsPickler.dll"
#r "../../bin/Thespian.dll"
#r "../../bin/Thespian.Tests.dll"

open System
open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Tests

let t = new ``Collocated BTcp``()

t.``ActorRef toUri <-> fromUri``()
