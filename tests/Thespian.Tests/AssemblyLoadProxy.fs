module Nessos.Thespian.Tests.AssemblyLoadProxy

open System
open System.Reflection
open System.Threading.Tasks
open MBrace.FsPickler

#if NETCOREAPP
open System.Runtime.Loader

/// An assembly load context that mirrors assembly loading from the currently running context
type MirroredAssemblyLoadContext() =
    inherit AssemblyLoadContext()
        
    let tryResolveFileName (an : AssemblyName) =
        AppDomain.CurrentDomain.GetAssemblies()
        |> Array.tryFind (fun a -> a.GetName() = an)
        |> Option.filter (fun a -> not a.IsDynamic)
        |> Option.map (fun a -> a.Location)
            
    override this.Load an =
        match tryResolveFileName an with
        | None -> null
        | Some path -> this.LoadFromAssemblyPath path
        
[<AbstractClass>]
type LoadContextProxy<'a, 'b>() =
    abstract Dispose : unit -> unit
    abstract SendAndReceive : 'a -> Async<'b>
    interface IDisposable with member __.Dispose() = __.Dispose()
        
    member private this.SendAndReceiveMarshalled (requestBytes : byte[]) =
        MarshallingHelper.SendAndReceiveMarshalled this requestBytes
            
and private MarshallingHelper private () =
    static let serializer = FsPickler.CreateBinarySerializer()
        
    static member SendAndReceiveMarshalled (proxy : LoadContextProxy<'a,'b>) (requestBytes : byte[]) =
        let body = async {
            let request = serializer.UnPickle requestBytes
            let! response = proxy.SendAndReceive request
            return serializer.Pickle response
        }

        Async.StartAsTask body
            
    static member CreateLocalWrapper (remoteHandle : obj) =
        let remoteMethod = remoteHandle.GetType().GetMethod("SendAndReceiveMarshalled", BindingFlags.NonPublic ||| BindingFlags.Instance)
        { new LoadContextProxy<'a, 'b>() with
            override __.Dispose() = (remoteHandle :?> IDisposable).Dispose()
            override __.SendAndReceive (request : 'a) = async {
                let requestBytes = serializer.Pickle request
                let responseTask = remoteMethod.Invoke(remoteHandle, [|requestBytes|]) :?> Task<byte[]>
                let! responseBytes = Async.AwaitTask responseTask
                return serializer.UnPickle<'b> responseBytes
            }
        }

type AssemblyLoadContext with
    member ctx.CreateProxy<'Proxy, 'a, 'b when 'Proxy : (new : unit -> 'Proxy) and 'Proxy :> LoadContextProxy<'a,'b>>() : LoadContextProxy<'a, 'b> =
        let _ = ctx.LoadFromAssemblyPath(typeof<MarshallingHelper>.Assembly.Location)
        let asm = ctx.LoadFromAssemblyPath(typeof<'Proxy>.Assembly.Location)
        let remoteContextType = asm.GetType typeof<'Proxy>.FullName
        let remoteInstance = Activator.CreateInstance(remoteContextType)
        MarshallingHelper.CreateLocalWrapper remoteInstance

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