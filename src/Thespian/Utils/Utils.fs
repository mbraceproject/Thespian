namespace Nessos.Thespian

    open System
    open System.IO
    open System.Reflection
    open System.Threading
    open System.Collections.Concurrent

    [<AutoOpen>]
    module internal Utils =

        /// detect the running version of F#
        let fsharpVersion = typeof<int option>.Assembly.GetName().Version

        let isFsharp31 = fsharpVersion >= System.Version("4.3.1")

        /// stackless raise operator
        //let inline raise (e: System.Exception) = (# "throw" e : 'T #)

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


        [<RequireQualifiedAccess>]
        module Choice =
            let split (inputs : Choice<'T, 'S> list) =
                let rec helper (ts, ss, rest) =
                    match rest with
                    | [] -> List.rev ts, List.rev ss
                    | Choice1Of2 t :: rest -> helper (t :: ts, ss, rest)
                    | Choice2Of2 s :: rest -> helper (ts, s :: ss, rest)

                helper ([], [], inputs)

            let splitArray (inputs: Choice<'T, 'U> []): 'T[] * 'U[] =
                    inputs |> Array.choose (function Choice1Of2 r -> Some r | _ -> None),
                    inputs |> Array.choose (function Choice2Of2 r -> Some r | _ -> None)


        type BinaryWriter with
            member writer.WriteByteArray(bytes: byte[]) =
                writer.Write(bytes.Length)
                writer.Write(bytes)

        type BinaryReader with
            member reader.ReadByteArray(): byte[] =
                let length = reader.ReadInt32()
                reader.ReadBytes(length)


        // http://t0yv0.blogspot.com/2012/07/speeding-up-f-printf.html

        type internal Cache<'T> private () =
            static let cache = new System.Collections.Concurrent.ConcurrentDictionary<_,_>()

            static member Format(format: Printf.StringFormat<'T>) : 'T =
                let key = format.Value
                let ok, value = cache.TryGetValue key
                if ok then value
                else
                    let f = sprintf format
                    let _ = cache.TryAdd(key, f)
                    f

        /// fast sprintf
        let sprintf fmt =
            if isFsharp31 then sprintf fmt
            else
                Cache<_>.Format(fmt)


        [<RequireQualifiedAccess>]
        module Seq =

            let ofOption (xs : seq<'a> option) : seq<'a> =
                match xs with
                | None -> seq []
                | Some xs -> xs

            let toOption (xs : seq<'a>) : seq<'a> option =
                if Seq.isEmpty xs then None else Some xs

            let tryHead (xs: seq<'a>): 'a option =
                if Seq.isEmpty xs then None else xs |> Seq.head |> Some

        [<RequireQualifiedAccess>]
        module Option =

            let filter f x =
                match x with
                | None -> None
                | Some x -> if f x then Some x else None

            let ofNullable<'T when 'T : null> (x : 'T) = 
                match x with null -> None | x -> Some x

            /// returns the head of a list if nonempty
            let ofList = function [] -> None | h :: _ -> Some h

            /// match t with None -> s | Some t0 -> f t0
            let bind2 (f : 'T -> 'S) (s : 'S) (t : 'T option) =
                match t with None -> s | Some t0 -> f t0


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
