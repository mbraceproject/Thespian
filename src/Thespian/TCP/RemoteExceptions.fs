namespace Nessos.Thespian.Remote

    open System
    open System.Net
    open System.Runtime.Serialization

    open Nessos.Thespian
    open Nessos.Thespian.Tools

    [<AutoOpen>]
    module Utils = 
#if PROTOCOLTRACE
        let sprintfn fmt = 
            Printf.ksprintf Console.WriteLine fmt
#else
        let sprintfn fmt = Printf.ksprintf ignore fmt
#endif

    [<Serializable>]
    type TcpProtocolConfigurationException =
        inherit Exception

        new(message: string) = {
            inherit Exception(message)
        }

        new(message: string, innerException: Exception) = {
            inherit Exception(message, innerException)
        }

        public new(info: SerializationInfo, context: StreamingContext) =
            { inherit Exception(info, context) }

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) = 
                base.GetObjectData(info, context)

    type SocketListenerException = 
        inherit CommunicationException

        val private endPoint: IPEndPoint

        new(message: string, ipEndPoint: IPEndPoint) = {
            inherit CommunicationException(message)

            endPoint = ipEndPoint
        }

        new(message: string,ipEndPoint: IPEndPoint, innerException: Exception) = {
            inherit CommunicationException(message, innerException)

            endPoint = ipEndPoint
        }

        internal new(info: SerializationInfo, context: StreamingContext) = { 
            inherit CommunicationException(info, context) 

            endPoint = info.GetValue("endPoint", typeof<IPEndPoint>) :?> IPEndPoint
        }

        member e.EndPoint = e.endPoint

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) =
                info.AddValue("endPoint", e.endPoint)