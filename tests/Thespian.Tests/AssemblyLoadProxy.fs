module Nessos.Thespian.Tests.AssemblyLoadProxy

open System
open System.Reflection
open System.Threading
open System.Threading.Tasks
open MBrace.FsPickler

#if NETCOREAPP
open System.Runtime.Loader
open System.Runtime.ExceptionServices

/// An assembly load context that mirrors assembly loading from the currently running context
type MirroredAssemblyLoadContext() =
    inherit AssemblyLoadContext(isCollectible = true)
        
    let currentLoadContext = AssemblyLoadContext.GetLoadContext(Assembly.GetExecutingAssembly())

    let tryResolveFileName (an : AssemblyName) =
        currentLoadContext.Assemblies
        |> Seq.tryFind (fun a -> a.FullName = an.FullName)
        |> Option.filter (fun a -> not a.IsDynamic)
        |> Option.map (fun a -> a.Location)
            
    override this.Load an =
        match tryResolveFileName an with
        | None -> null
        | Some path -> this.LoadFromAssemblyPath path

    interface IDisposable with
        member this.Dispose() = this.Unload()

type ILoadContextProxy<'T when 'T :> IDisposable> =
    inherit IDisposable
    abstract Execute : command:('T -> 'R) -> 'R
    abstract ExecuteAsync : command:('T -> Async<'R>) -> Async<'R>

type private LoadContextMarshaller<'T when 'T : (new : unit -> 'T) and 'T :> IDisposable> () =
    let instance = new 'T()

    static let pickler = FsPickler.CreateBinarySerializer()
    static let rethrow (e : exn) = ExceptionDispatchInfo.Capture(e).Throw() ; Unchecked.defaultof<_>

    interface IDisposable with member __.Dispose() = instance.Dispose()

    member __.ExecuteMarshalled (bytes : byte[]) : byte[] = 
        let result = 
            try 
                let command = pickler.UnPickle<'T -> obj> bytes
                command instance |> Choice1Of2 
            with e -> Choice2Of2 e

        pickler.Pickle<Choice<obj,exn>> result

    member __.ExecuteMarshalledAsync (ct : CancellationToken, bytes : byte[]) : Task<byte[]> = 
        let work = async {
            let executor = async { 
                let command = pickler.UnPickle<'T -> Async<obj>> bytes 
                return! command instance 
            }

            let! result = Async.Catch executor
            return pickler.Pickle<Choice<obj, exn>> result
        } 
    
        Async.StartAsTask(work, cancellationToken = ct)

    static member CreateProxyFromMarshallerHandle (remoteHandle : obj) =
        let remoteMethod = remoteHandle.GetType().GetMethod("ExecuteMarshalled", BindingFlags.NonPublic ||| BindingFlags.Instance)
        let remoteAsyncMethod = remoteHandle.GetType().GetMethod("ExecuteMarshalledAsync", BindingFlags.NonPublic ||| BindingFlags.Instance)
        { new ILoadContextProxy<'T> with
            member __.Dispose() = (remoteHandle :?> IDisposable).Dispose()
            member __.Execute<'R> (command : 'T -> 'R) =
                let boxedCommand (instance:obj) = let result = command (instance :?> 'T) in result :> obj
                let commandBytes = pickler.Pickle<'T -> obj> boxedCommand
                let responseBytes = remoteMethod.Invoke(remoteHandle, [|commandBytes|]) :?> byte[]
                match pickler.UnPickle<Choice<obj,exn>> responseBytes with
                | Choice1Of2 obj -> obj :?> 'R
                | Choice2Of2 e -> rethrow e

            member __.ExecuteAsync<'R> (command : 'T -> Async<'R>) = async {
                let! ct = Async.CancellationToken
                let boxedCommand instance = async { let! result = command instance in return box result }
                let commandBytes = pickler.Pickle<'T -> Async<obj>> boxedCommand
                let responseTask = remoteAsyncMethod.Invoke(remoteHandle, [|ct ; commandBytes|]) :?> Task<byte[]>
                let! responseBytes = Async.AwaitTask responseTask
                return 
                    match pickler.UnPickle<Choice<obj, exn>> responseBytes with
                    | Choice1Of2 obj -> obj :?> 'R
                    | Choice2Of2 e -> rethrow e
            }
        }


type AssemblyLoadContext with
    member ctx.CreateProxy<'T when 'T : (new : unit -> 'T) and 'T :> IDisposable>() : ILoadContextProxy<'T> =
        let getRemoteType (t : Type) =
            let remoteAssembly = ctx.LoadFromAssemblyPath t.Assembly.Location
            remoteAssembly.GetType t.FullName

        // Construct the type of LoadContextMarshaller<'TProxy>, but using assemblies loaded in the remote context
        let remoteProxyType = getRemoteType typeof<'T>
        let remoteMarshallerType = getRemoteType (typeof<LoadContextMarshaller<'T>>.GetGenericTypeDefinition())
        let remoteInstanceType = remoteMarshallerType.MakeGenericType remoteProxyType

        // instantiate proxy in remote context
        let remoteInstanceCtor = remoteInstanceType.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public, null, [||], null)
        let remoteInstance = remoteInstanceCtor.Invoke [||]
        LoadContextMarshaller<'T>.CreateProxyFromMarshallerHandle remoteInstance
        

#else

open System.Security.Policy

type AppDomain with
    /// Create a new AppDomain with supplied configuration
    static member CreateNew(friendlyName : string, ?evidence, ?setup, ?permissions) =
        let currentDomain = AppDomain.CurrentDomain
        let appDomainSetup = defaultArg setup currentDomain.SetupInformation
        let permissions = defaultArg permissions currentDomain.PermissionSet
        let evidence = defaultArg evidence (Evidence currentDomain.Evidence)
        AppDomain.CreateDomain(friendlyName, evidence, appDomainSetup, permissions)

    /// Instantiates a value of supplied type in the remote AppDomain
    member appDomain.CreateInstance<'T when 'T :> MarshalByRefObject
                                        and 'T : (new : unit -> 'T)> () =

        let assemblyName = typeof<'T>.Assembly.FullName
        let typeName = typeof<'T>.FullName
        let culture = System.Globalization.CultureInfo.CurrentCulture
        let bindingFlags = BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance
        let handle = appDomain.CreateInstance(assemblyName, typeName, false, bindingFlags, null, [||], culture, [||])
        handle.Unwrap() :?> 'T
#endif