namespace Nessos.Thespian

    open System
    open System.IO
    open System.Reflection
    open System.Threading
    open System.Collections.Concurrent

    [<AutoOpen>]
    module internal Utils =

        /// stackless raise operator
        let inline raise (e: System.Exception) = (# "throw" e : 'T #)

        let private remoteStackTraceField : FieldInfo =
            let bfs = BindingFlags.NonPublic ||| BindingFlags.Instance
            match typeof<System.Exception>.GetField("remote_stack_trace", bfs) with
            | null ->
                match typeof<System.Exception>.GetField("_remoteStackTraceString", bfs) with
                | null -> failwith "Could not locate RemoteStackTrace field for System.Exception."
                | f -> f
            | f -> f

        let inline raiseWithStackTrace (trace : string) (e : #exn) =
            do remoteStackTraceField.SetValue(e, trace)
            raise e

        let inline reraise' (e : #exn) = raiseWithStackTrace (e.StackTrace + System.Environment.NewLine) e

        let memoize f =
            let cache = new ConcurrentDictionary<_,_>()

            fun x ->
                let found, y = cache.TryGetValue x
                if found then y
                else
                    let y = f x
                    let _ = cache.TryAdd(x, y)
                    y

        let compareOn (f: 'T -> 'U when 'U : comparison) (x: 'T) (other: obj): int =
            match other with
            | :? 'T as y -> compare (f x) (f y)
            | _ -> invalidArg "other" "Unable to compare values of incompatible types."

        let hashOn (f: 'T -> 'U when 'U : equality) (x: 'T): int = hash (f x)

        let equalsOn (f: 'T -> 'U when 'U : equality) (x: 'T) (other: obj): bool =
            match other with
            | :? 'T as y -> (f x = f y)
            | _ -> false

        let equalsOnComparison (f: 'T -> 'U when 'U : comparison) (x: 'T) (other: obj): bool =
            match other with
            | :? 'T as y -> compareOn f x y = 0
            | _ -> false


        type BinaryWriter with
            member writer.WriteByteArray(bytes: byte[]) =
                writer.Write(bytes.Length)
                writer.Write(bytes)

        type BinaryReader with
            member reader.ReadByteArray(): byte[] =
                let length = reader.ReadInt32()
                reader.ReadBytes(length)


        module RegExp =

            open System
            open System.Text.RegularExpressions

            let (|Match|_|) =
                let regex = memoize(fun pattern -> Regex(pattern))
            
                fun (pat : string) (inp : string) ->
                    let m = (regex pat).Match inp in
                    if m.Success 
                    then Some (List.tail [ for g in m.Groups -> g.Value ])
                    else None