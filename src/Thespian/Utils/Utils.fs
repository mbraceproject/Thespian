namespace Nessos.Thespian.Utils

open System
open System.Reflection
open System.Collections.Concurrent
open System.Runtime.Serialization

/// Collection of general-purpose utility functions
[<AutoOpen>]
module Utility =

    /// detect the running version of F#
    let internal fsharpVersion = typeof<int option>.Assembly.GetName().Version

    /// Returns true if runtime F# version is at least 3.1
    let internal isFsharp31 = fsharpVersion >= System.Version("4.3.1")

    /// <summary>
    ///     Thread-safe memoization combinator.
    /// </summary>
    /// <param name="f">Function to be memoized.</param>
    let memoize (f : 'a -> 'b) =
        let cache = new ConcurrentDictionary<'a,'b>()
        fun x -> cache.GetOrAdd(x, f)

    // http://t0yv0.blogspot.com/2012/07/speeding-up-f-printf.html
    type private Cache<'T> private () =
        static let cache = new System.Collections.Concurrent.ConcurrentDictionary<_,_>()

        static member Format(format: Printf.StringFormat<'T>) : 'T =
            let key = format.Value
            let ok, value = cache.TryGetValue key
            if ok then value
            else
                let f = sprintf format
                let _ = cache.TryAdd(key, f)
                f

    /// <summary>
    ///     Fast sprintf function; used instead of inefficient sprintf found in FSharp.Core v.3.0 and below.
    /// </summary>
    let sprintf fmt =
        if isFsharp31 then sprintf fmt
        else Cache<_>.Format(fmt)

    type SerializationInfo with
        /// <summary>
        ///     Write value to SerializationInfo.
        /// </summary>
        /// <param name="id">value identifier.</param>
        /// <param name="value">value to be written.</param>
        member inline sI.Write<'T> (id : string) (value : 'T) =
            sI.AddValue(id, value, typeof<'T>)

        /// <summary>
        ///     Read value from SerializationInfo.
        /// </summary>
        /// <param name="id">value identifier.</param>
        member inline sI.Read<'T>(id : string) =
            sI.GetValue(id, typeof<'T>) :?> 'T


/// A collection of utilities for authoring F# classes
[<RequireQualifiedAccess>]
module FSharpClass =

    /// <summary>
    ///     Comparison by projection.
    /// </summary>
    /// <param name="proj">Projection function.</param>
    /// <param name="this">this value.</param>
    /// <param name="that">that value.</param>
    let inline compareBy (proj : 'T -> 'U) (this : 'T) (that : obj) : int =
        match that with
        | :? 'T as that -> compare (proj this) (proj that)
        | _ -> invalidArg "that" <| sprintf "invalid comparand %A." that

    /// <summary>
    ///     Hashcode by projection.
    /// </summary>
    /// <param name="proj">Projection function.</param>
    /// <param name="this">this value.</param>
    let inline hashBy (proj : 'T -> 'U) (this : 'T) : int = hash (proj this)

    /// <summary>
    ///     Equality by projection.
    /// </summary>
    /// <param name="proj">Projection function.</param>
    /// <param name="this">this value.</param>
    /// <param name="that">that value.</param>
    let inline equalsBy (proj : 'T -> 'U) (this : 'T) (that : obj): bool =
        match that with
        | :? 'T as that -> proj this = proj that
        | _ -> false

    /// <summary>
    ///     Equality by comparison projection.
    /// </summary>
    /// <param name="proj">Projection function.</param>
    /// <param name="this">this value.</param>
    /// <param name="that">that value.</param>
    let inline equalsByComparison (proj : 'T -> 'U) (this : 'T) (that : obj) : bool =
        match that with
        | :? 'T as that -> compareBy proj this that = 0
        | _ -> false

/// Exception handling utilities
module Control =

    open System.Runtime.ExceptionServices

    /// <summary>
    ///     Reraise operator that can be used everywhere.
    /// </summary>
    /// <param name="e">exception to be reraised.</param>
    let inline reraise' (e : #exn) : 'T = 
        let edi = ExceptionDispatchInfo.Capture e
        edi.Throw()
        Unchecked.defaultof<'T> // force a generic type, won't be called

/// Extensions for F# Choice types
[<RequireQualifiedAccess>]
module Choice =
    
    /// <summary>
    ///     Split a list of Choice values.
    /// </summary>
    /// <param name="inputs">choice values.</param>
    let split (inputs : Choice<'T, 'S> list) =
        let rec helper (ts, ss, rest) =
            match rest with
            | [] -> List.rev ts, List.rev ss
            | Choice1Of2 t :: rest -> helper (t :: ts, ss, rest)
            | Choice2Of2 s :: rest -> helper (ts, s :: ss, rest)

        helper ([], [], inputs)

    /// <summary>
    ///     Split an array of Choice values.
    /// </summary>
    /// <param name="inputs">choice values.</param>
    let splitArray (inputs: Choice<'T, 'U> []): 'T[] * 'U[] =
        inputs |> Array.choose (function Choice1Of2 r -> Some r | _ -> None),
            inputs |> Array.choose (function Choice2Of2 r -> Some r | _ -> None)

/// IEnumerable extensions
[<RequireQualifiedAccess>]
module Seq =

    /// <summary>
    ///     Try reading the head of given sequence.
    /// </summary>
    /// <param name="xs">Input sequence.</param>
    let tryHead (xs: seq<'a>) : 'a option =
        if Seq.isEmpty xs then None else xs |> Seq.head |> Some

/// F# option extensions
[<RequireQualifiedAccess>]
module Option =

    /// <summary>
    ///     Returns 'Some x' iff f x is satisfied.
    /// </summary>
    /// <param name="f">predicate to be evaluated.</param>
    /// <param name="opt">Input optional.</param>
    let filter f opt =
        match opt with
        | None -> None
        | Some x -> if f x then opt else None

    /// <summary>
    ///     Returns 'Some t' iff t is not null.
    /// </summary>
    /// <param name="t">Value to be examined.</param>
    let ofNull<'T when 'T : not struct> (t : 'T) = 
        if obj.ReferenceEquals(t, null) then None
        else
            Some t

    /// <summary>
    ///     Attempt to return the head of a list.
    /// </summary>
    /// <param name="xs">Input list.</param>
    let ofList xs = match xs with [] -> None | h :: _ -> Some h

    /// <summary>
    ///     match t with None -> s | Some t0 -> f t0
    /// </summary>
    /// <param name="f">Mapping function.</param>
    /// <param name="s">Default value.</param>
    /// <param name="t">Optional input.</param>
    let bind2 (f : 'T -> 'S) (s : 'S) (t : 'T option) =
        match t with None -> s | Some t0 -> f t0

/// Regular Expression extensions
[<RequireQualifiedAccess>]
module RegEx =

    open System.Text.RegularExpressions

    let private regexMemo = memoize(fun pattern -> new Regex(pattern))

    /// <summary>
    ///     Memoized RegEx matching. 
    ///     Returns the values of the first matching pattern and its groupings.
    /// </summary>
    /// <param name="pattern">RegEx pattern.</param>
    /// <param name="input">input text.</param>
    let tryMatch (pattern:string) (input:string) =
        let m = (regexMemo pattern).Match input
        if m.Success then
            Some [ for g in m.Groups -> g.Value ]
        else
            None

    /// <summary>
    ///     Memoized RegEx matching. 
    ///     Returns all matches in text for pattern.
    /// </summary>
    /// <param name="pattern">RegEx pattern.</param>
    /// <param name="input">input text.</param>
    let tryMatches (pattern:string) (input:string) =
        let matches = (regexMemo pattern).Matches input
        if matches.Count = 0 then None
        else
            Some [ for m in matches -> m.Value ]

    /// <summary>
    ///     Memoized RegEx active pattern.
    ///     Returns the values of the first matching pattern and its groupings.
    /// </summary>
    /// <param name="pattern">RegEx pattern.</param>
    /// <param name="input">input text.</param>
    let (|Match|_|) (pattern : string) (input : string) = tryMatch pattern input

    /// <summary>
    ///     Memoized RegEx active pattern.
    ///     Returns all matches in text for pattern.
    /// </summary>
    /// <param name="pattern">RegEx pattern.</param>
    /// <param name="input">input text.</param>
    let (|Matches|_|) (pattern : string) (input : string) = tryMatches pattern input