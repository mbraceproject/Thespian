namespace Nessos.Thespian.Cluster

open Nessos.Thespian
open Nessos.Thespian.ImemDb

type NodeSelectionStrategy(numOfNodes: int) =
    interface IActivationStrategy with
        override __.GetActivationNodesAsync(clusterState: ClusterState, instanceId: int, definition: Definition) = async {
            return 
                Query.from clusterState.Db.ClusterNode
                |> Query.leftOuterJoin clusterState.Db.ClusterActivation <@ fun (clusterNode, clusterActivation) -> clusterNode.NodeManager = clusterActivation.NodeManager @>
                |> Query.where <@ fun (_, clusterActivation) -> if clusterActivation.IsSome then 
                                                                    clusterActivation.Value.ActivationReference.Definition = definition.Path
                                                                    && clusterActivation.Value.ActivationReference.InstanceId = instanceId
                                                                else true @>
                |> Query.toSeq
                |> Seq.map (fun (clusterNode, _) -> clusterNode.NodeManager)
                |> Seq.distinct
                |> Seq.truncate numOfNodes
                |> Seq.toList
        }

type ClusterWideActivation() =
    interface IActivationStrategy with
        override __.GetActivationNodesAsync(clusterState: ClusterState, instanceId: int, definition: Definition) = async {
            let slaveNodes =
                Query.from clusterState.Db.ClusterNode
                |> Query.toSeq
                |> Seq.map (fun node -> node.NodeManager)
                |> Seq.toList

            return clusterState.MasterNode::slaveNodes
        }

type SpecificNodeStrategy(nodeManager: ActorRef<NodeManager>) =
    interface IActivationStrategy with
        override __.GetActivationNodesAsync(_, _, _) = async { return [nodeManager] }

type MasterNodeStrategy() =
    interface IActivationStrategy with
        override __.GetActivationNodesAsync(clusterState: ClusterState, _, _) = async {
            return [clusterState.MasterNode]
        }


module ActivationStrategy =
    let collocated = new Collocated() :> IActivationStrategy
    let selectNodes num = new NodeSelectionStrategy(num) :> IActivationStrategy
    let clusterWide = new ClusterWideActivation() :> IActivationStrategy
    let specificNode (nodeManager: #ActorRef<NodeManager>) = new SpecificNodeStrategy(nodeManager) :> IActivationStrategy
    let masterNode = new MasterNodeStrategy() :> IActivationStrategy

