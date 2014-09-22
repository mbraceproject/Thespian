namespace Nessos.Thespian.Cluster.Tests

open System
open NUnit.Framework
open FsUnit


[<TestFixture>]
type BaseLifetimeTests() =

    static do System.Threading.ThreadPool.SetMinThreads(100,100) |> ignore

    let timeoutPerNode = 15 * 1000 // in ms

    member __.ClusterSizes = [1; 2; 4; 8]
    
    [<Test>]
    member __.``Simple node(s) spawn``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes
        ()

    [<Test>]
    member __.``Simple node(s) boot``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes
        testCluster.Timeout <- numberOfNodes * timeoutPerNode
        testCluster.Boot(0, 0) |> should equal () 

    [<Test>]
    member __.``Simple node(s) boot and shutdown``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes
        testCluster.Timeout <- numberOfNodes * timeoutPerNode
        testCluster.Boot(0, 0) |> should equal () 
        testCluster.Shutdown() |> should equal () 

    [<Test>]
    member __.``Simple node(s) boot and reboot``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes
        testCluster.Timeout <- numberOfNodes * timeoutPerNode
        testCluster.Boot(0, 0) |> should equal () 
        testCluster.Reboot()   |> should equal () 

    [<Test>]
    member __.``Simple node(s) boot, reboot, shutdown``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes
        testCluster.Timeout <- numberOfNodes * timeoutPerNode
        testCluster.Boot(0, 0) |> should equal () 
        testCluster.Reboot()   |> should equal () 
        testCluster.Shutdown() |> should equal () 
