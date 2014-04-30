#r "bin/debug/Nessos.Thespian.dll"
#r "bin/debug/Nessos.Thespian.Remote.dll"


#r "bin/debug/Nessos.Thespian.Cluster.dll"
#r "bin/debug/Unquote.dll"
#r "bin/debug/Nessos.MBrace.ImemDB.dll"


open Nessos.Thespian
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Remote.ConnectionPool
open Nessos.Thespian.Remote.TcpProtocol.Unidirectional
open Nessos.Thespian.Cluster
open Nessos.MBrace.ImemDb


TcpListenerPool.DefaultHostname <- "localhost"
TcpListenerPool.RegisterListener(4242, concurrentAccepts = 10)
TcpConnectionPool.Init()

let defSerializerName = Serialization.SerializerRegistry.GetDefaultSerializer().Name

//Test ActorRef<'T> and ReliableActorRef<'T> equality

let actor: Actor<int> = 
    Actor.empty()
    |> Actor.rename "test"
    |> Actor.publish [UTcp()]
    |> Actor.start

let actorRef1 = actor.Ref
let actorRef2 = 
    let b = Serialization.SerializerRegistry.GetDefaultSerializer().Serialize(obj(), actorRef1)
    Serialization.SerializerRegistry.GetDefaultSerializer().Deserialize(obj(), b) :?> ActorRef<int>

actorRef1 = actorRef2
actorRef2 = actorRef1

let ractorRef1 = ReliableActorRef.FromRef actorRef1
let ractorRef2 = ReliableActorRef.FromRef actorRef2

ractorRef1 = ractorRef2
ractorRef2 = ractorRef1
ractorRef1 :> ActorRef<int> = actorRef1
ractorRef2 :> ActorRef<int> = actorRef2



let actorUri uri = sprintf "%s/%s" uri defSerializerName

let actorRefs : ActorRef<NodeManager> list = 
    [ "utcp://laptop:49644/*/nodeManager.NodeManager"
      "utcp://laptop:49628/*/nodeManager.NodeManager"
      "utcp://laptop:49636/*/nodeManager.NodeManager"
      "utcp://laptop:49652/*/nodeManager.NodeManager" ]
    |> List.map (actorUri >> ActorRef.fromUri)

let nodes = 
    actorRefs |> List.map (fun n -> { NodeManager = n; TimeAdded = System.DateTime.Now }) 
              |> Table.ofSeq <@ fun (cn: ClusterNode) -> cn.NodeManager @>

let rec comparisons (n: ActorRef<NodeManager>) (ns: ActorRef<NodeManager> list) =
    match ns with
    | n'::ns' -> 
        printfn "%A comp %A = %d" n.Id n'.Id (n.CompareTo(n'))
        comparisons n ns'
    | [] -> ()
let ns = nodes.DataMap |> Map.toList |> List.map snd |> List.map (fun cn -> cn.NodeManager)
ns |> List.iter (fun n -> comparisons n ns)

let dead = List.nth actorRefs 0
let nodes' = nodes |> Table.remove dead

nodes.DataMap |> Map.toList |> List.map snd |> List.exists (fun cn -> cn.NodeManager = dead)
nodes'.DataMap |> Map.toList |> List.map snd |> List.forall (fun cn -> cn.NodeManager <> dead)


Query.from nodes
|> Query.toSeq
|> Seq.map (fun cn -> cn.NodeManager)
|> List.ofSeq
