#r "bin/Debug/FsPickler.dll"
#r "bin/Debug/Thespian.dll"

#load "../../tests/Thespian.Tests/TestDefinitions.fs"

open Nessos.Thespian
open Nessos.Thespian.Tests.TestDefinitions
open Nessos.Thespian.Tests.TestDefinitions.Remote

//let appDomainManager = new AppDomainManager<UtcpActorManagerFactory>("TestDomain")
let f: Actor<TestMessage<int, int>> -> Async<unit> = Behavior.stateful 0 Behaviors.state
//let actorManager = appDomainManager.Factory.CreateActorManager<TestMessage<int, int>>(BehaviorValue.Create<TestMessage<int, int>>(f))

let bin = BehaviorValue.Create<TestMessage<int, int>>(f)
let f' = bin.Unwrap()

