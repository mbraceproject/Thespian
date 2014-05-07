namespace Nessos.Thespian

open System
open System.Runtime.Serialization        

//TODO!!!
//Change ActorExceptions to have ActorId field, instead of UUID and name

[<Serializable>]
type ActorException =
  inherit Exception

  val private actorName: string
  val private actorId: ActorId option

  new (message: string, actorName: string) =
    {
      inherit Exception(message)
      actorName = actorName
      actorId = None
    }
  new (message: string, actorId: ActorId) =
    {
      inherit Exception(message)
      actorName = actorId.Name
      actorId = Some actorId
    }

  new (message: string, actorName: string, innerException: Exception) =
    {
      inherit Exception(message, innerException)
      actorName = actorName
      actorId = None
    }
  new (message: string, actorId: ActorId, innerException: Exception) =
    {
      inherit Exception(message, innerException)
      actorName = actorId.Name
      actorId = Some actorId
    }

  new (message: string) = ActorException(message, String.Empty)
  new (message: string, innerException: Exception) = ActorException(message, String.Empty, innerException)

  new (info: SerializationInfo, context: StreamingContext) =
    { 
      inherit Exception(info, context)
      actorName = info.GetString("actorName")
      actorId = info.GetValue("actorId", typeof<ActorId option>) :?> ActorId option
    }

  member self.ActorName = self.actorName
  member self.ActorId = self.actorId

  override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
    info.AddValue("actorName", self.actorName)
    base.GetObjectData(info, context)

  interface ISerializable with
    override self.GetObjectData(info: SerializationInfo, context: StreamingContext) = self.GetObjectData(info, context)


[<Sealed; Serializable>]
type MessageHandlingException =
  inherit ActorException

  new (message: string, innerException: Exception) = { inherit ActorException(message, innerException) }
  new (message: string, actorName: string, innerException: Exception) = { inherit ActorException(message, actorName, innerException) }
  new (message: string, actorId: ActorId, innerException: Exception) = { inherit ActorException(message, actorId, innerException) }

  private new (info: SerializationInfo, context: StreamingContext) =  { inherit ActorException(info, context) }

  interface ISerializable with
    override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
      base.GetObjectData(info, context)


[<Sealed; Serializable>]
type ActorInactiveException =
  inherit ActorException

  new (message: string) = { inherit ActorException(message) }
  new (message: string, actorName: string) = { inherit ActorException(message, actorName) }
  new (message: string, actorId: ActorId) = { inherit ActorException(message, actorId) }
  new (message: string, innerException: Exception) = { inherit ActorException(message, innerException) }
  new (message: string, actorName: string, innerException: Exception) = { inherit ActorException(message, actorName, innerException) }

  private new (info: SerializationInfo, context: StreamingContext) = { inherit ActorException(info, context) }

  interface ISerializable with
    override self.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)


[<Sealed; Serializable>]
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
