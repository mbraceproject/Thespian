namespace Nessos.Thespian

open System
open Nessos.Thespian.Utils

[<AbstractClass>]
[<Serializable>]
type ActorId(actorName : string) = 
    member __.Name = actorName
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

type Reply<'T> = 
    | Value of 'T
    | Exception of exn
with
    member self.GetValue(?keepOriginalStackTrace : bool) = 
        match self with
        | Value t -> t
        | Exception e -> 
            if defaultArg keepOriginalStackTrace false then reraise' e
            else raise e

type LogLevel = 
    | Info
    | Warning
    | Error
with
    override self.ToString() =
        match self with
        | Info -> "INFO"
        | Warning -> "WARNING"
        | Error -> "ERROR"

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
    static member Register(logger : ILogger) = lock logger (fun () -> loggerContainer := Some logger)
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
