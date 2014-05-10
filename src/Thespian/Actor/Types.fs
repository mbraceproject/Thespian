namespace Nessos.Thespian

    open System

    type ActorUUID = Guid

    [<AbstractClass>]
    [<Serializable>]
    type ActorId(actorName: string) =
      member __.Name = actorName
      
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


    type Reply<'T> = 
        | Value of 'T
        | Exception of exn
    with
        member r.GetValue(?keepOriginalStackTrace : bool) =
            match r with
            | Value t -> t
            | Exception e ->
                if defaultArg keepOriginalStackTrace false then
                    reraise' e
                else
                    raise e

    type LogLevel =
        | Info
        | Warning
        | Error

    type LogSource =
        | Actor of string
        | Protocol of string
    
    type Log<'T> = LogLevel * LogSource * 'T
    type Log = Log<obj>


    // in-place replacement for MBrace's logger interface: temporary solution

    type ILogger =
        abstract Log : msg:string * lvl:LogLevel * time:DateTime -> unit     
        
    type Logger private () =
        static let loggerContainer = ref None  

        static member Register(logger : ILogger) =
            lock logger (fun () -> loggerContainer := Some logger)

        static member DefaultLogger =
            match loggerContainer.Value with
            | None -> invalidOp "Thespian: no logger has been registered."
            | Some l -> l

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module Log =
        
        let log time lvl msg = Logger.DefaultLogger.Log(msg, lvl, time)

        let logNow lvl msg = Logger.DefaultLogger.Log(msg, lvl, DateTime.Now)

        let logInfo msg = Logger.DefaultLogger.Log(msg, Info, DateTime.Now)

        let logWarning msg = Logger.DefaultLogger.Log(msg, Warning, DateTime.Now)

        let logError msg = Logger.DefaultLogger.Log(msg, Error, DateTime.Now)

        let logException (e : exn) msg =
            let message = sprintf "%s\n    Exception=%O" msg e
            Logger.DefaultLogger.Log(message, Error, DateTime.Now)
