namespace Nessos.Thespian

open System
open System.Runtime.Serialization        


/// <summary>
///     Base Thespian exception
/// </summary>
[<Serializable>]
type ThespianException =
    inherit Exception
    
    val private actorName: string
    val private actorId: ActorId option
    
    /// <summary>
    ///     Constructs a new ThespianException.
    /// </summary>
    /// <param name="message">Exception message.</param>
    /// <param name="actorName">Actor name.</param>
    new (message: string, actorName: string) =
        {
            inherit Exception(message)
            actorName = actorName
            actorId = None
        }


    /// <summary>
    ///     Constructs a new ThespianException by ActorId.
    /// </summary>
    /// <param name="message">Exception message.</param>
    /// <param name="actorId">Actor Id.</param>
    new (message: string, actorId: ActorId) =
        {
            inherit Exception(message)
            actorName = actorId.Name
            actorId = Some actorId
        }
    
    /// <summary>
    ///     Constructs a new ThespianException.
    /// </summary>
    /// <param name="message">Exception message.</param>
    /// <param name="actorName">Actor name.</param>
    /// <param name="innerException">Inner exception.</param>
    new (message: string, actorName: string, innerException: Exception) =
        {
            inherit Exception(message, innerException)
            actorName = actorName
            actorId = None
        }

    /// <summary>
    ///     Construct a new Exception by ActorId.
    /// </summary>
    /// <param name="message">Exception message.</param>
    /// <param name="actorId">Actor id.</param>
    /// <param name="innerException">Inner exception.</param>
    new (message: string, actorId: ActorId, innerException: Exception) =
       {
           inherit Exception(message, innerException)
           actorName = actorId.Name
           actorId = Some actorId
       }
    
    /// <summary>
    ///     Constructs a new ThespianException with no actor name.
    /// </summary>
    /// <param name="message"></param>
    new (message: string) = ThespianException(message, String.Empty)

    /// <summary>
    ///     Constructs a new ThespianException with no actor name.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="innerException"></param>
    new (message: string, innerException: Exception) = ThespianException(message, String.Empty, innerException)
    
    /// <summary>
    ///     ISerializable ThespianException constructor.
    /// </summary>
    /// <param name="info">Serialization info.</param>
    /// <param name="context">Streaming context.</param>
    new (info: SerializationInfo, context: StreamingContext) =
        { 
            inherit Exception(info, context)
            actorName = info.GetString("actorName")
            actorId = info.GetValue("actorId", typeof<ActorId option>) :?> ActorId option
        }
    
    /// Actor name that raised the exception.
    member self.ActorName = self.actorName

    /// Actor id that raised the exception.
    member self.ActorId = self.actorId
    
    override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
        info.AddValue("actorName", self.actorName)
        info.AddValue("actorId", self.ActorId)
        base.GetObjectData(info, context)
    
    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) = self.GetObjectData(info, context)

/// Serialization operations enumeration.
type SerializationOperation =
    | Serialization = 0
    | Deserialization = 1

/// Serialization exception raised by Thespian
[<Sealed; Serializable>]
type ThespianSerializationException =
    inherit ThespianException
    
    val private serializationOperation: SerializationOperation
    
    internal new (message: string, operation: SerializationOperation) =
        { 
            inherit ThespianException(message)
            serializationOperation = operation
        }

    internal new (message: string, operation: SerializationOperation, innerException: Exception) =
        { 
            inherit ThespianException(message, innerException)
            serializationOperation = operation
        }

    internal new (message: string, operation: SerializationOperation, actorName: string, innerException: Exception) =
        { 
            inherit ThespianException(message, actorName, innerException)
            serializationOperation = operation
        }

    internal new (message: string, operation: SerializationOperation, actorId: ActorId, innerException: Exception) =
        { 
            inherit ThespianException(message, actorId, innerException)
            serializationOperation = operation
        }

    private new (info: SerializationInfo, context: StreamingContext) =
        {  
            inherit ThespianException(info, context)
            serializationOperation = info.GetValue("serializationOperation", typeof<SerializationOperation>) :?> SerializationOperation
        }

    member self.SerializationOperation =  self.serializationOperation
    
    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            info.AddValue("serializationOperation", self.serializationOperation)
            base.GetObjectData(info, context)


/// <summary>
///     Raised when message reply channels are passed exceptions.
/// </summary>
[<Sealed; Serializable>]
type MessageHandlingException =
    inherit ThespianException
  
    internal new (message: string, innerException: Exception) = { inherit ThespianException(message, innerException) }
    internal new (message: string, actorName: string, innerException: Exception) = { inherit ThespianException(message, actorName, innerException) }
    internal new (message: string, actorId: ActorId, innerException: Exception) = { inherit ThespianException(message, actorId, innerException) }
  
    private new (info: SerializationInfo, context: StreamingContext) =  { inherit ThespianException(info, context) }
  
    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            base.GetObjectData(info, context)

/// <summary>
///     Raised when attempting to post to inactive actor.
/// </summary>
[<Sealed; Serializable>]
type ActorInactiveException =
    inherit ThespianException
    
    internal new (message: string) = { inherit ThespianException(message) }
    internal new (message: string, actorName: string) = { inherit ThespianException(message, actorName) }
    internal new (message: string, actorId: ActorId) = { inherit ThespianException(message, actorId) }
    internal new (message: string, innerException: Exception) = { inherit ThespianException(message, innerException) }
    internal new (message: string, actorName: string, innerException: Exception) = { inherit ThespianException(message, actorName, innerException) }
    
    private new (info: SerializationInfo, context: StreamingContext) = { inherit ThespianException(info, context) }
    
    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)


/// <summary>
///     Raised when attempting to post to a failed actor.
/// </summary>
[<Sealed; Serializable>]
type ActorFailedException =
    inherit ThespianException

    internal new (message: string) = { inherit ThespianException(message) }
    internal new (message: string, innerException: Exception) = { inherit ThespianException(message, innerException) }
    internal new (message: string, actorName: string) = { inherit ThespianException(message, actorName) }
    internal new (message: string, actorName: string, innerException: Exception) = { inherit ThespianException(message, actorName, innerException) }
    internal new (message: string, actorId: ActorId) = { inherit ThespianException(message, actorId) }
    internal new (message: string, actorId: ActorId, innerException: Exception) = { inherit ThespianException(message, actorId, innerException) }

    private new (info: SerializationInfo, context: StreamingContext) = { inherit ThespianException(info, context) }

    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)

/// <summary>
///     Raised on Thespian protocol communication failure.
/// </summary>
[<Serializable>]
type CommunicationException =
    inherit ThespianException
    
    new (message: string) = { inherit ThespianException(message) }
    new (message: string, innerException: Exception) = { inherit ThespianException(message, innerException) }
    new (message: string, actorName: string) = { inherit ThespianException(message, actorName) }
    new (message: string, actorName: string, innerException: Exception) = { inherit ThespianException(message, actorName, innerException) }
    new (message: string, actorId: ActorId) = { inherit ThespianException(message, actorId) }
    new (message: string, actorId: ActorId, innerException: Exception) = { inherit ThespianException(message, actorId, innerException) }
    
    new (info: SerializationInfo, context: StreamingContext) = { inherit ThespianException(info, context) }
    
    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)

/// <summary>
///     Raised on unknown recipient actor.
/// </summary>
[<Sealed ; Serializable>]
type UnknownRecipientException =
    inherit CommunicationException
    
    internal new (message: string) = { inherit CommunicationException(message) }
    internal new (message: string, innerException: Exception) = { inherit CommunicationException(message, innerException) }
    internal new (message: string, actorName: string) = { inherit CommunicationException(message, actorName) }
    internal new (message: string, actorName: string, innerException: Exception) = { inherit CommunicationException(message, actorName, innerException) }
    internal new (message: string, actorId: ActorId) = { inherit CommunicationException(message, actorId) }
    internal new (message: string, actorId: ActorId, innerException: Exception) = { inherit CommunicationException(message, actorId, innerException) }
    
    private new (info: SerializationInfo, context: StreamingContext) = { inherit CommunicationException(info, context) }
    
    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)

/// <summary>
///     Raised on failed delivery.
/// </summary>
[<Sealed; Serializable>]
type DeliveryException =
    inherit CommunicationException
    
    internal new (message: string) = { inherit CommunicationException(message) }
    internal new (message: string, innerException: Exception) = { inherit CommunicationException(message, innerException) }
    internal new (message: string, actorName: string) = { inherit CommunicationException(message, actorName) }
    internal new (message: string, actorName: string, innerException: Exception) = { inherit CommunicationException(message, actorName, innerException) }
    internal new (message: string, actorId: ActorId) = { inherit CommunicationException(message, actorId) }
    internal new (message: string, actorId: ActorId, innerException: Exception) = { inherit CommunicationException(message, actorId, innerException) }
    
    private new (info: SerializationInfo, context: StreamingContext) = { inherit CommunicationException(info, context) }
    
    interface ISerializable with
        override __.GetObjectData(info: SerializationInfo, context: StreamingContext) = base.GetObjectData(info, context)

/// Timeout type enumeration.
type TimeoutType =
    | Connection = 1
    | MessageWrite = 2
    | MessageRead = 3
    | ConfirmationRead = 4
    | ConfirmationWrite = 5

/// <summary>
///     Raised on protocol communication timeouts.
/// </summary>
[<Sealed ; Serializable>]
type CommunicationTimeoutException =
    inherit CommunicationException
    
    val timeoutType: TimeoutType
    
    internal new (message: string, timeoutType: TimeoutType) =
        {
            inherit CommunicationException(message)
            timeoutType = timeoutType
        }
    internal new (message: string, timeoutType: TimeoutType, innerException: Exception) =
        {
            inherit CommunicationException(message, innerException)
            timeoutType = timeoutType
        }
    internal new (message: string, actorName: string, timeoutType: TimeoutType) =
        {
            inherit CommunicationException(message, actorName)
            timeoutType = timeoutType
        }
    internal new (message: string, actorName: string, timeoutType: TimeoutType, innerException: Exception) =
        {
            inherit CommunicationException(message, actorName, innerException)
            timeoutType = timeoutType
        }
    internal new (message: string, actorId: ActorId, timeoutType: TimeoutType) =
        {
            inherit CommunicationException(message, actorId)
            timeoutType = timeoutType
        }
    internal new (message: string, actorId: ActorId, timeoutType: TimeoutType, innerException: Exception) =
        {
            inherit CommunicationException(message, actorId, innerException)
            timeoutType = timeoutType
        }
    
    private new (info: SerializationInfo, context: StreamingContext) =
        {
            inherit CommunicationException(info, context)
            timeoutType = info.GetValue("timeoutType", typeof<TimeoutType>) :?> TimeoutType
        }
    
    /// Type of timeout that caused the exception
    member self.TimeoutType = self.TimeoutType
    
    interface ISerializable with
        override self.GetObjectData(info: SerializationInfo, context: StreamingContext) =
            info.AddValue("timeoutType", self.timeoutType)
            base.GetObjectData(info, context)

/// <summary>
///     Raised on invalid actor configuration
/// </summary>
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

/// A collection of active patterns for handling Thespian exceptions.
[<AutoOpen>]
module ExceptionHelpers =

    /// MessageHandlingException active pattern
    let (|MessageHandlingException|_|) (e : exn) = 
        match e with
        | :? MessageHandlingException as e -> Some(e.Message, e.InnerException)
        | _ -> None

    /// recursively traverses through MessageHandlingExceptions, 
    /// retrieving the innermost exception raised by the original actor.
    let (|MessageHandlingExceptionRec|_|) (e : exn) =
        let rec aux (e : MessageHandlingException) =
            match e.InnerException with
            | :? MessageHandlingException as e -> aux e
            | _ -> Some(e.Message, e.InnerException)

        match e with
        | :? MessageHandlingException as e -> aux e
        | _ -> None
    
    /// CommunicationTimeoutException active pattern
    let (|CommunicationTimeoutException|_|) (e : exn) = 
        match e with
        | :? CommunicationTimeoutException as e -> Some(e.Message, e.TimeoutType, e.InnerException)
        | _ -> None
    
    /// CommunicationException active pattern
    let rec (|CommunicationException|_|) (e : exn) = 
        match e with
        | :? CommunicationException as e -> Some(e.Message, e.InnerException)
        | MessageHandlingException(_, CommunicationException(m, inner)) -> Some(m, inner)
        | _ -> None
    
    /// UnknownRecipientException active pattern
    let rec (|UnknownRecipientException|_|) (e : exn) = 
        match e with
        | :? UnknownRecipientException as e -> Some(e.Message, e.InnerException)
        | MessageHandlingException(_, UnknownRecipientException(m, inner)) -> Some(m, inner)
        | _ -> None
    
    /// DeliveryException active pattern
    let rec (|DeliveryException|_|) (e : exn) = 
        match e with
        | :? DeliveryException as e -> Some(e.Message, e.InnerException)
        | MessageHandlingException(_, DeliveryException(m, inner)) -> Some(m, inner)
        | _ -> None
    
    /// Inner exception active pattern
    let (|InnerException|_|) (e : exn) = 
        match e with
        | null -> None
        | _ -> Some e
