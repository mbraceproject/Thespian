namespace Nessos.Thespian

    open System

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