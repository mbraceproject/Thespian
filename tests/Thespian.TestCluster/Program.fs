module Nessos.Thespian.TestCluster.Service

open System

open Nessos.Thespian
open Nessos.Thespian.Logging
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Cluster

type ConsoleLogger() =
    interface ILogger with
        override __.Log(msg : string, lvl : LogLevel, time : DateTime) =
            Console.WriteLine(sprintf "%A :: %O :: %s" time lvl msg)

let mainLoop () : 'T =
    let rec loop () = async {
        do! Async.Sleep 2000
        return! loop ()
    }

    Async.RunSynchronously(loop())

[<EntryPoint>]
let main argv =
    let startupReceiverUri = argv.[0]
    
    Logging.DefaultLogger <- ConsoleLogger()

    TcpListenerPool.DefaultHostname <- "localhost"                                    
    TcpListenerPool.RegisterListener(0)

    let port = TcpListenerPool.GetListener().LocalEndPoint.Port

    let nodeConfiguration = {
        NodeAddress = Address.AnyHost(port)
        DefinitionRegistry = DefinitionRegistry.registry
        EventManager = new TestClusterNodeEventManager()
        ActivationPatterns = []
    }

    NodeManager.initNode nodeConfiguration |> Async.Start

    let startupReceiver = ActorRef.fromUri startupReceiverUri
    startupReceiver <-- port

    mainLoop()
    
    0
