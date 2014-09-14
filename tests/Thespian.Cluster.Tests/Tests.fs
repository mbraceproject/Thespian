namespace Nessos.Thespian.Cluster.Tests

open System
open NUnit.Framework


[<TestFixture>]
type BaseLifetimeTests() =

    member __.ClusterSizes = [1; 2; 4; 8]
    
    [<Test>]
    member __.``Simple node(s) spawn``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes
        ()


    [<Test>]
    member __.``Simple node(s) boot``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes

        testCluster.Boot(0, 0)

    [<Test>]
    member __.``Simple node(s) boot and shutdown``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes

        testCluster.Boot(0, 0)
        testCluster.Shutdown()

    [<Test>]
    member __.``Simple node(s) boot and reboot``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes

        testCluster.Boot(0, 0)
        testCluster.Reboot()

    [<Test>]
    member __.``Simple node(s) boot, reboot, shutdown``([<ValueSource("ClusterSizes")>] numberOfNodes : int) =
        use testCluster = TestCluster.spawn numberOfNodes

        testCluster.Boot(0, 0)
        testCluster.Reboot()
        testCluster.Shutdown()
