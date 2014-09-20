namespace Nessos.Thespian

open System

/// <summary>
///     Abstract unique actor Identifier
/// </summary>
[<AbstractClass ; Serializable>]
type ActorId(actorName : string) = 
    
    /// Actor Name
    member __.Name = actorName

    /// ActorId comparison implementation
    abstract CompareTo : ActorId -> int

    override self.CompareTo(other : ActorId) = self.ToString().CompareTo(other.ToString())
    override self.GetHashCode() = Unchecked.hash (self.ToString())
    
    override self.Equals(other : obj) = 
        match other with
        | :? ActorId as otherActorId -> self.CompareTo(otherActorId) = 0
        | _ -> false
    
    interface IComparable<ActorId> with
        member self.CompareTo(otherActorId : ActorId) : int = self.CompareTo(otherActorId)
    
    interface IComparable with
        member self.CompareTo(other : obj) : int = 
            match other with
            | :? ActorId as otherId -> self.CompareTo(otherId)
            | _ -> invalidArg "other" "Cannot compare objects of incompatible types."