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
        | Actor of string * ActorUUID
        | Protocol of string
    
    type Log<'T> = LogLevel * LogSource * 'T
    type Log = Log<obj>
