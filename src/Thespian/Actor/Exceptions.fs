namespace Nessos.Thespian

open System
open System.Runtime.Serialization        

[<Serializable>]
type ThespianException =
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
    
    new (message: string) = ThespianException(message, String.Empty)
    new (message: string, innerException: Exception) = ThespianException(message, String.Empty, innerException)
    
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
        info.AddValue("actorId", self.ActorId)
        base.GetObjectData(info, context)
    
    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) = self.GetObjectData(info, context)


type SerializationOperation =
    | Serialization = 0
    | Deserialization = 1

[<Sealed; Serializable>]
type ThespianSerializationException =
    inherit ThespianException
    
    val private serializationOperation: SerializationOperation
    
    new (message: string, operation: SerializationOperation) =
        { 
            inherit ThespianException(message)
            serializationOperation = operation
        }
    new (message: string, operation: SerializationOperation, innerException: Exception) =
        { 
            inherit ThespianException(message, innerException)
            serializationOperation = operation
        }
    new (message: string, operation: SerializationOperation, actorName: string, innerException: Exception) =
        { 
            inherit ThespianException(message, actorName, innerException)
            serializationOperation = operation
        }
    new (message: string, operation: SerializationOperation, actorId: ActorId, innerException: Exception) =
        { 
            inherit ThespianException(message, actorId, innerException)
            serializationOperation = operation
        }
    private new (info: SerializationInfo, context: StreamingContext) =
        {  
            inherit ThespianException(info, context)
            serializationOperation = info.GetValue("serializationOperation", typeof<SerializationOperation>) :?> SerializationOperation
        }
    
    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            info.AddValue("serializationOperation", self.serializationOperation)
            base.GetObjectData(info, context)


[<Sealed; Serializable>]
type MessageHandlingException =
    inherit ThespianException
  
    new (message: string, innerException: Exception) = { inherit ThespianException(message, innerException) }
    new (message: string, actorName: string, innerException: Exception) = { inherit ThespianException(message, actorName, innerException) }
    new (message: string, actorId: ActorId, innerException: Exception) = { inherit ThespianException(message, actorId, innerException) }
  
    private new (info: SerializationInfo, context: StreamingContext) =  { inherit ThespianException(info, context) }
  
    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            base.GetObjectData(info, context)


[<Sealed; Serializable>]
type ActorInactiveException =
    inherit ThespianException
    
    new (message: string) = { inherit ThespianException(message) }
    new (message: string, actorName: string) = { inherit ThespianException(message, actorName) }
    new (message: string, actorId: ActorId) = { inherit ThespianException(message, actorId) }
    new (message: string, innerException: Exception) = { inherit ThespianException(message, innerException) }
    new (message: string, actorName: string, innerException: Exception) = { inherit ThespianException(message, actorName, innerException) }
    
    private new (info: SerializationInfo, context: StreamingContext) = { inherit ThespianException(info, context) }
    
    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)


[<Sealed; Serializable>]
type ActorFailedException =
    inherit ThespianException

    new (message: string) = { inherit ThespianException(message) }
    new (message: string, innerException: Exception) = { inherit ThespianException(message, innerException) }
    new (message: string, actorName: string) = { inherit ThespianException(message, actorName) }
    new (message: string, actorName: string, innerException: Exception) = { inherit ThespianException(message, actorName, innerException) }
    new (message: string, actorId: ActorId) = { inherit ThespianException(message, actorId) }
    new (message: string, actorId: ActorId, innerException: Exception) = { inherit ThespianException(message, actorId, innerException) }

    private new (info: SerializationInfo, context: StreamingContext) = { inherit ThespianException(info, context) }

    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)

    
[<Serializable>]
type CommunicationException =
    inherit ThespianException
    
    new (message: string) = { inherit ThespianException(message) }
    new (message: string, innerException: Exception) = { inherit ThespianException(message, innerException) }
    new (message: string, actorName: string) = { inherit ThespianException(message, actorName) }
    new (message: string, actorName: string, innerException: Exception) = { inherit ThespianException(message, actorName, innerException) }
    new (message: string, actorId: ActorId) = { inherit ThespianException(message, actorId) }
    new (message: string, actorId: ActorId, innerException: Exception) = { inherit ThespianException(message, actorId, innerException) }
    
    public new (info: SerializationInfo, context: StreamingContext) = { inherit ThespianException(info, context) }
    
    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)

[<Serializable>]
type UnknownRecipientException =
    inherit CommunicationException
    
    new (message: string) = { inherit CommunicationException(message) }
    new (message: string, innerException: Exception) = { inherit CommunicationException(message, innerException) }
    new (message: string, actorName: string) = { inherit CommunicationException(message, actorName) }
    new (message: string, actorName: string, innerException: Exception) = { inherit CommunicationException(message, actorName, innerException) }
    new (message: string, actorId: ActorId) = { inherit CommunicationException(message, actorId) }
    new (message: string, actorId: ActorId, innerException: Exception) = { inherit CommunicationException(message, actorId, innerException) }
    
    public new (info: SerializationInfo, context: StreamingContext) = { inherit CommunicationException(info, context) }
    
    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)

[<Serializable>]
type DeliveryException =
    inherit CommunicationException
    
    new (message: string) = { inherit CommunicationException(message) }
    new (message: string, innerException: Exception) = { inherit CommunicationException(message, innerException) }
    new (message: string, actorName: string) = { inherit CommunicationException(message, actorName) }
    new (message: string, actorName: string, innerException: Exception) = { inherit CommunicationException(message, actorName, innerException) }
    new (message: string, actorId: ActorId) = { inherit CommunicationException(message, actorId) }
    new (message: string, actorId: ActorId, innerException: Exception) = { inherit CommunicationException(message, actorId, innerException) }
    
    public new (info: SerializationInfo, context: StreamingContext) = { inherit CommunicationException(info, context) }
    
    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)

type TimeoutType =
    | Connection = 1
    | MessageWrite = 2
    | MessageRead = 3
    | ConfirmationRead = 4
    | ConfirmationWrite = 5

[<Serializable>]
type CommunicationTimeoutException =
    inherit CommunicationException
    
    val timeoutType: TimeoutType
    
    new (message: string, timeoutType: TimeoutType) =
        {
            inherit CommunicationException(message)
            timeoutType = timeoutType
        }
    new (message: string, timeoutType: TimeoutType, innerException: Exception) =
        {
            inherit CommunicationException(message, innerException)
            timeoutType = timeoutType
        }
    new (message: string, actorName: string, timeoutType: TimeoutType) =
        {
            inherit CommunicationException(message, actorName)
            timeoutType = timeoutType
        }
    new (message: string, actorName: string, timeoutType: TimeoutType, innerException: Exception) =
        {
            inherit CommunicationException(message, actorName, innerException)
            timeoutType = timeoutType
        }
    new (message: string, actorId: ActorId, timeoutType: TimeoutType) =
        {
            inherit CommunicationException(message, actorId)
            timeoutType = timeoutType
        }
    new (message: string, actorId: ActorId, timeoutType: TimeoutType, innerException: Exception) =
        {
            inherit CommunicationException(message, actorId, innerException)
            timeoutType = timeoutType
        }
    
    public new (info: SerializationInfo, context: StreamingContext) =
        {
            inherit CommunicationException(info, context)
            timeoutType = info.GetValue("timeoutType", typeof<TimeoutType>) :?> TimeoutType
        }
    
    member self.TimeoutType = self.TimeoutType
    
    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            info.AddValue("timeoutType", self.timeoutType)
            base.GetObjectData(info, context)

[<Serializable>]
type ActorConfigurationException =
    inherit ThespianException

    new (message: string) = { inherit ThespianException(message) }
    new (message: string, innerException: Exception) = { inherit ThespianException(message, innerException) }
    new (message: string, actorName: string) = { inherit ThespianException(message, actorName) }
    new (message: string, actorName: string, innerException: Exception) = { inherit ThespianException(message, actorName, innerException) }
    new (message: string, actorId: ActorId) = { inherit ThespianException(message, actorId) }
    new (message: string, actorId: ActorId, innerException: Exception) = { inherit ThespianException(message, actorId, innerException) }

    public new  (info: SerializationInfo, context: StreamingContext) = { inherit ThespianException(info, context) }

    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)

[<AutoOpen>]
module ExceptionHelpers = 
    let (|MessageHandlingException|_|) (e : exn) = 
        match e with
        | :? MessageHandlingException as e -> Some(e.Message, e.InnerException)
        | _ -> None
    
    let (|CommunicationTimeoutException|_|) (e : exn) = 
        match e with
        | :? CommunicationTimeoutException as e -> Some(e.Message, e.TimeoutType, e.InnerException)
        | _ -> None
    
    let rec (|CommunicationException|_|) (e : exn) = 
        match e with
        | :? CommunicationException as e -> Some(e.Message, e.InnerException)
        | MessageHandlingException(_, CommunicationException(m, inner)) -> Some(m, inner)
        | _ -> None
    
    let rec (|UnknownRecipientException|_|) (e : exn) = 
        match e with
        | :? UnknownRecipientException as e -> Some(e.Message, e.InnerException)
        | MessageHandlingException(_, UnknownRecipientException(m, inner)) -> Some(m, inner)
        | _ -> None
    
    let rec (|DeliveryException|_|) (e : exn) = 
        match e with
        | :? DeliveryException as e -> Some(e.Message, e.InnerException)
        | MessageHandlingException(_, DeliveryException(m, inner)) -> Some(m, inner)
        | _ -> None
    
    let (|InnerException|_|) (e : exn) = 
        match e with
        | null -> None
        | _ -> Some e


