namespace Nessos.Thespian

    open Nessos.Thespian.Utils.Control

    /// Represents the result of a computation that
    /// can either be a value or an exception.
    type Result<'T> = 
        | Ok of 'T
        | Exn of exn
    with
        member self.Value =
            match self with
            | Ok t -> t
            | Exn e -> reraise' e

    /// Untyped Result value.
    and Result = Result<obj>

    /// Result auxiliary functions
    [<RequireQualifiedAccess>]
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module Result =

        /// <summary>
        ///     Declare a succesful result.
        /// </summary>
        /// <param name="value">Result value.</param>
        let inline value<'T> (value : 'T) = Result<'T>.Ok value

        /// <summary>
        ///     Declare an exceptional result.
        /// </summary>
        /// <param name="exn">Exception</param>
        let inline exn<'T> (exn : exn) = Result<'T>.Exn exn

        /// <summary>
        ///     Protects a delayed computation.
        /// </summary>
        /// <param name="f">Computation to be evaluated.</param>
        let protect (f : unit -> 'T) =
            try f () |> Ok with e -> Exn e

        /// <summary>
        ///     Result map combinator.
        /// </summary>
        /// <param name="mapF">Map function.</param>
        /// <param name="tresult">Input result.</param>
        let map (mapF : 'T -> 'S) (tresult : Result<'T>) =
            match tresult with
            | Ok x -> Ok (mapF x)
            | Exn e -> Exn e

        /// <summary>
        ///     Map function that catches any exception raised by continuation.
        /// </summary>
        /// <param name="f">Continuation function.</param>
        /// <param name="tresult">Input result.</param>
        let bind (f : 'T -> 'S) (tresult : Result<'T>) =
            match tresult with
            | Ok x -> try Ok <| f x with e -> Exn e
            | Exn e -> Exn e

        /// <summary>
        ///     Boxes a result value.
        /// </summary>
        /// <param name="result">Result value.</param>
        let box (result : Result<'T>)  : Result =
            match result with
            | Ok t -> Ok (box t)
            | Exn e -> Exn e

        /// <summary>
        ///     Unboxes a result value.
        /// </summary>
        /// <param name="result">Result value.</param>
        let unbox<'T> (result : Result) =
            match result with
            | Ok o -> Ok (unbox<'T> o)
            | Exn e -> Exn e