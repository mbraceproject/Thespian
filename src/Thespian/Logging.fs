module Nessos.Thespian.Logging

    open System

    /// LogLevel of a log event.
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

    /// Specifies the log source of a log event.
    and LogSource = 
        | Actor of string
        | Protocol of string

    /// Actor Log event type
    and Log = LogLevel * LogSource * obj

    /// Abstract logger implementation
    type ILogger = 

        /// <summary>
        ///     Log a new entry.
        /// </summary>
        /// <param name="message">Log message.</param>
        /// <param name="level">Log level.</param>
        /// <param name="time">Log time.</param>
        abstract Log : message:string * level:LogLevel * time:DateTime -> unit

    /// A logger implementation that does nothing.
    type NullLogger () =
        interface ILogger with
            member __.Log (_,_,_) = ()

    /// Gets or sets the default logger implementation used by Thespian.
    let mutable DefaultLogger : ILogger = new NullLogger () :> _


    /// A collection of logging functions that write entries to the default logger.

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module Log =

        /// Log new entry to default logger
        let log time level message = DefaultLogger.Log(message, level, time)

        /// Log new entry to default logger
        let logNow level message = DefaultLogger.Log(message, level, DateTime.Now)

        /// Log info to default logger
        let logInfo message = DefaultLogger.Log(message, Info, DateTime.Now)

        /// Log warning to default logger
        let logWarning message = DefaultLogger.Log(message, Warning, DateTime.Now)

        /// Log error to default logger
        let logError message = DefaultLogger.Log(message, Error, DateTime.Now)
    
        /// Log exception to default logger
        let logException (exn : exn) message = 
            let message = sprintf "%s\n    Exception=%O" message exn
            DefaultLogger.Log(message, Error, DateTime.Now)