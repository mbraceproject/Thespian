namespace Nessos.Thespian.Cluster.Tests

open System
open System.Diagnostics
open Nessos.Thespian
open Nessos.Thespian.Utils.Async
open Nessos.Thespian.Remote
open Nessos.Thespian.Cluster

type Node() =
    let mutable killed = false
    
    let onMono = match Type.GetType("Mono.Runtime") with
                 | null -> false
                 | _ -> true
            
    let executable = System.IO.Path.Combine(System.IO.Directory.GetCurrentDirectory(), "Thespian.TestCluster.exe")
    
    let command =
        if onMono then "mono"
        else executable

    let receiver = Receiver.create()
                   |> Receiver.publish [Protocols.utcp()]
                   |> Receiver.start
    let receiverUri = ActorRef.toUri receiver.Ref
    let awaitNodePort = Async.AwaitObservable(receiver |> Receiver.toObservable, 10000)

    let args =
        if onMono then
            executable + " " + receiverUri
        else receiverUri

    let startInfo = new ProcessStartInfo(command, args)

    let osProcess =
        startInfo.UseShellExecute <- false
        startInfo.CreateNoWindow <- true
        startInfo.RedirectStandardOutput <- true
        startInfo.RedirectStandardError <- true

        Process.Start(startInfo)

    let nodePort = Async.RunSynchronously awaitNodePort

#if DEBUG
    let d1 = osProcess.OutputDataReceived.Subscribe(fun (args : DataReceivedEventArgs) -> Console.WriteLine args.Data)
    let d2 = osProcess.ErrorDataReceived.Subscribe(fun (args : DataReceivedEventArgs) -> Console.Error.WriteLine args.Data)
#endif

    do
        osProcess.EnableRaisingEvents <- true
#if DEBUG        
        osProcess.BeginOutputReadLine()
        osProcess.BeginErrorReadLine()
#endif
        receiver.Stop()

    member __.Port = nodePort

    member __.NodeManager =
        let uri = sprintf "utcp://localhost:%d/nodeManager" nodePort
        ActorRef.fromUri uri : ActorRef<NodeManager>

    member __.Kill() =
        if not killed then
#if DEBUG
            d1.Dispose()
            d2.Dispose()
#endif
            osProcess.Kill()
            killed <- true

    interface IDisposable with override __.Dispose() = __.Kill()
        


type ClusterController(nodes: Node list) =
    let clusterId = "TestCluster"
    let clusterManager =
        let clusterManagerNode = nodes.Head
        let uri = sprintf "utcp://localhost:%d/clusterManager.TestCluster" clusterManagerNode.Port
        ActorRef.fromUri uri :> ActorRef<ClusterManager>

    let nodeManagers = nodes |> List.map (fun node -> node.NodeManager)

    let mutable currentReplicationFactor = 0
    let mutable currentFailoverFactor = 0
     
    let mutable timeout = 5 * 1000

    member __.Timeout with get () = timeout and set t = timeout <- t
        
    member __.KillCluster() = for node in nodes do node.Kill()

    member __.ClusterManager = clusterManager

    member self.Boot(replicationFactor: int, failoverFactor: int) =
        let clusterConfiguration = {
            ClusterId = clusterId
            Nodes = nodeManagers |> List.tail |> List.toArray
            ReplicationFactor = replicationFactor
            FailoverFactor = failoverFactor
            NodeDeadNotify = fun _ -> async.Zero()
        }
        nodeManagers.Head.PostWithReply((fun ch -> InitCluster(ch, clusterConfiguration)), timeout = timeout)
        |> Async.Ignore
        |> Async.RunSynchronously
        currentReplicationFactor <- replicationFactor
        currentFailoverFactor <- failoverFactor

    member __.Shutdown() = clusterManager.PostWithReply(KillClusterSync, timeout = timeout) 
                           |> Async.Ignore
                           |> Async.RunSynchronously 

    member self.Reboot(?replicationFactor: int, ?failoverFactor: int) =
        let replicationFactor = defaultArg replicationFactor currentReplicationFactor
        let failoverFactor = defaultArg failoverFactor currentFailoverFactor
        self.Shutdown()
        self.Boot(replicationFactor, failoverFactor)

    interface IDisposable with override __.Dispose() = __.KillCluster()


module TestCluster =
    let mutable initPort = 4242
    
    let spawn (numberOfNodes : int) =
        let nodes = [ for i in 1..numberOfNodes -> new Node() ]
        new ClusterController(nodes)
