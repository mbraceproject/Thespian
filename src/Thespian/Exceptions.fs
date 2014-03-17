namespace Nessos.Thespian

    open System
    open System.Runtime.Serialization

    type ActorUUID = Guid
    
    //type ActorId = string
    [<AbstractClass>]
    [<Serializable>]
    type ActorId() =
        abstract CompareTo: ActorId -> int
        default actorId.CompareTo(other: ActorId) = actorId.ToString().CompareTo(other.ToString())

        override actorId.GetHashCode() = Unchecked.hash(actorId.ToString())
        override actorId.Equals(other: obj) =
            match other with
            | :? ActorId as otherActorId -> actorId.CompareTo(otherActorId) = 0
            | _ -> false

        interface IComparable<ActorId> with
            member actorId.CompareTo(otherActorId: ActorId): int = actorId.CompareTo(otherActorId)

        interface IComparable with
            member actorId.CompareTo(other: obj): int =
                match other with
                | :? ActorId as otherId -> actorId.CompareTo(otherId)
                | _ -> invalidArg "other" "Cannot compare objects of incompatible types."
        

    //TODO!!!
    //Change ActorExceptions to have ActorId field, instead of UUID and name

    [<Serializable>]
    type ActorException =
        inherit Exception

        val private actorName: string
        val private actorUUID: ActorUUID

        new(message: string, actorName: string, actorId: ActorUUID) = {
            inherit Exception(message)

            actorName = actorName
            actorUUID = actorId
        }

        new(message: string, actorName: string, actorId: ActorUUID, innerException: Exception) = {
            inherit Exception(message, innerException)

            actorName = actorName
            actorUUID = actorId
        }

        new(message: string) = ActorException(message, "", ActorUUID.Empty)

        new(message: string, innerException: Exception) = ActorException(message, "", ActorUUID.Empty, innerException)

        new(info: SerializationInfo, context: StreamingContext) = { 
            inherit Exception(info, context)

            actorName = info.GetString("actorName")
            actorUUID = info.GetValue("actorId", typeof<ActorUUID>) :?> ActorUUID
        }

        member e.ActorName = e.actorName
        member e.ActorId = e.actorUUID

        override e.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            info.AddValue("actorName", e.actorName)
            info.AddValue("actorId", e.actorUUID)

            base.GetObjectData(info, context)

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) =
                e.GetObjectData(info, context)


    [<Sealed>][<Serializable>]
    type MessageHandlingException =
        inherit ActorException

        new(message: string, innerException: Exception) = {
            inherit ActorException(message, innerException)
        }

        new(message: string, actorName: string, actorId: ActorUUID, innerException: Exception) = {
            inherit ActorException(message, actorName, actorId, innerException)
        }

        private new(info: SerializationInfo, context: StreamingContext) = 
            { inherit ActorException(info, context) }

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) =
                base.GetObjectData(info, context)



    [<Sealed>][<Serializable>]
    type ActorInactiveException =
        inherit ActorException

        new(message: string) = {
            inherit ActorException(message)
        }

        new(message: string, actorName: string, actorId: ActorUUID) = {
            inherit ActorException(message, actorName, actorId)
        }

        new(message: string, innerException: Exception) = {
            inherit ActorException(message, innerException)
        }

        new(message: string, actorName: string, actorId: ActorUUID, innerException: Exception) = {
            inherit ActorException(message, actorName, actorId, innerException)
        }

        private new(info: SerializationInfo, context: StreamingContext) = 
            { inherit ActorException(info, context) }

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) =    
                base.GetObjectData(info, context)

    [<Sealed>][<Serializable>]
    type ActorFailedException =
        inherit ActorException

        new(message: string) = {
            inherit ActorException(message)
        }

        new(message: string, innerException: Exception) = {
            inherit ActorException(message, innerException)
        }

        new(message: string, actorName: string, actorId: ActorUUID) = {
            inherit ActorException(message, actorName, actorId)
        }

        new(message: string, actorName: string, actorId: ActorUUID, innerException: Exception) = {
            inherit ActorException(message, actorName, actorId, innerException)
        }

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) = 
                base.GetObjectData(info, context)
    
    [<Serializable>]
    type CommunicationException = 
        inherit ActorException

        new(message: string) = {
            inherit ActorException(message)
        }

        new(message: string, innerException: Exception) = {
            inherit ActorException(message, innerException)
        }

        new(message: string, actorName: string, actorId: ActorUUID) = {
            inherit ActorException(message, actorName, actorId)
        }

        new(message: string, actorName: string, actorId: ActorUUID, innerException: Exception) = {
            inherit ActorException(message, actorName, actorId, innerException)
        }

        public new(info: SerializationInfo, context: StreamingContext) =
            { inherit ActorException(info, context) }

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) = 
                base.GetObjectData(info, context)

    [<Serializable>]
    type UnknownRecipientException = 
        inherit CommunicationException

        new(message: string) = {
            inherit CommunicationException(message)
        }

        new(message: string, innerException: Exception) = {
            inherit CommunicationException(message, innerException)
        }

        new(message: string, actorName: string, actorId: ActorUUID) = {
            inherit CommunicationException(message, actorName, actorId)
        }

        new(message: string, actorName: string, actorId: ActorUUID, innerException: Exception) = {
            inherit CommunicationException(message, actorName, actorId, innerException)
        }

        public new(info: SerializationInfo, context: StreamingContext) =
            { inherit CommunicationException(info, context) }

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) = 
                base.GetObjectData(info, context)

    [<Serializable>]
    type DeliveryException = 
        inherit CommunicationException

        new(message: string) = {
            inherit CommunicationException(message)
        }

        new(message: string, innerException: Exception) = {
            inherit CommunicationException(message, innerException)
        }

        new(message: string, actorName: string, actorId: ActorUUID) = {
            inherit CommunicationException(message, actorName, actorId)
        }

        new(message: string, actorName: string, actorId: ActorUUID, innerException: Exception) = {
            inherit CommunicationException(message, actorName, actorId, innerException)
        }

        public new(info: SerializationInfo, context: StreamingContext) =
            { inherit CommunicationException(info, context) }

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) = 
                base.GetObjectData(info, context)

    [<Serializable>]
    type ActorConfigurationException = 
        inherit ActorException

        new(message: string) = {
            inherit ActorException(message)
        }

        new(message: string, innerException: Exception) = {
            inherit ActorException(message, innerException)
        }

        new(message: string, actorName: string, actorId: ActorUUID) = {
            inherit ActorException(message, actorName, actorId)
        }

        new(message: string, actorName: string, actorId: ActorUUID, innerException: Exception) = {
            inherit ActorException(message, actorName, actorId, innerException)
        }

        public new(info: SerializationInfo, context: StreamingContext) =
            { inherit ActorException(info, context) }

        interface ISerializable with
            member e.GetObjectData(info: SerializationInfo, context: StreamingContext) = 
                base.GetObjectData(info, context)

    [<AutoOpen>]
    module ExceptionHelpers =
        let (|MessageHandlingException|_|) (e: exn) =
            match e with
            | :? MessageHandlingException as e -> Some(e.Message, e.ActorId, e.ActorName, e.InnerException)
            | _ -> None

        let rec (|CommunicationException|_|) (e: exn) =
            match e with
            | :? CommunicationException as e -> Some(e.Message, e.ActorId, e.ActorName, e.InnerException)
            | MessageHandlingException(_, _, _, CommunicationException(m, id, n, inner)) -> Some(m, id, n, inner)
            | _ -> None

        let rec (|UnknownRecipientException|_|) (e: exn) =
            match e with
            | :? UnknownRecipientException as e -> Some(e.Message, e.ActorId, e.ActorName, e.InnerException)
            | MessageHandlingException(_, _, _, UnknownRecipientException(m, id, n, inner)) -> Some(m, id, n, inner)
            | _ -> None

        let rec (|DeliveryException|_|) (e: exn) =
            match e with
            | :? DeliveryException as e -> Some(e.Message, e.ActorId, e.ActorName, e.InnerException)
            | MessageHandlingException(_, _, _, DeliveryException(m, id, n, inner)) -> Some(m, id, n, inner)
            | _ -> None

        let (|InnerException|_|) (e: exn) =
            match e with
            | null -> None
            | _ -> Some e
