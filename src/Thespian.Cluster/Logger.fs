namespace Nessos.Thespian.Cluster

// Moved code to Thespian/Types.fs

//open System.Diagnostics
//open Nessos.MBrace.Utils
//
//let private logger = lazy IoC.Resolve<ILogger>()
//
//let msg logLevel msg = logger.Value.Log msg logLevel
//let entry entry = logger.Value.LogEntry entry
//let error e msg = logger.Value.LogError e msg
//let info msg = logger.Value.LogInfo msg
//let withException logLevel e msg = logger.Value.LogWithException e msg logLevel
//let format logLevel fmt = logger.Value.Logf logLevel fmt
//
//type LoggerTraceListener() =
//    inherit TraceListener()
//    let logger = lazy IoC.Resolve<ILogger>()
//
//    override __.Write(message: string) = logger.Value.Log message LogLevel.Info
//    override l.WriteLine(message: string) = logger.Value.Log message LogLevel.Info
