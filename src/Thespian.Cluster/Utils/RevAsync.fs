[<AutoOpen>]
module Nessos.Thespian.Cluster.RevAsync

open System
open System.Threading

open Nessos.Thespian.LazyList
open Nessos.Thespian.Utils.Async

type RevAsync<'T> = private { Expr : ReversibleExpr }

and ReversibleResult<'T> =
    | Success of 'T * Reversal // value * finalization
    | Failure of exn * Reversal * Reversal // exn * compensation * finalization
    | Cancelled of Reversal * Reversal // compensation * finalization
    | Fault of exn * Reversal * Reversal // exn * compensation * finalization

and Reversal =
    | RAtom of Async<unit>
    | RSequential of Reversal list
    | RParallel of Reversal list

and private Primitive = 
    { 
        Computation : Async<obj * Async<unit> * Async<unit>> // result * recovery * finalization
        Type : System.Type
    }

and private ReversibleExpr =
    // Syntactic Branches
    | Primitive of Primitive
    | Delay of (unit -> ReversibleExpr)
    | Bind of ReversibleExpr * (obj -> ReversibleExpr)
    | Combine of ReversibleExpr * ReversibleExpr
    | TryWith of ReversibleExpr * (exn -> ReversibleExpr)
    | TryFinally of ReversibleExpr * (unit -> unit)
    | BindDisposable of IDisposable * Type * (obj -> ReversibleExpr)
    | DoSequential of LazyList<ReversibleExpr>
    | RunParallel of Type * ReversibleExpr seq
    // execution specific branches
    | Continuation of (obj -> ReversibleExpr)
    | Combinable of ReversibleExpr
    | WithBlock of (exn -> ReversibleExpr)
    | FinallyBlock of (unit -> unit)
    | Value of obj * Type
    | Exception of exn

and private TrampolineResult =
    | TValue of obj * Type
    | TException of exn
    | TFault of exn
    | TCancelled

and RevAsyncBuilder() =
    static let lift (f: 'T -> RevAsync<'U>) (x : 'S) =
        let expr = f (x :> obj :?> 'T) in expr.Expr

    member __.Delay(f: unit -> RevAsync<'T>): RevAsync<'T> = { Expr = Delay(lift f) }

    member __.Return(v: 'T): RevAsync<'T> =
        let prim = 
            {
                Computation = async { return v :> obj, async.Zero(), async.Zero() }
                Type = typeof<'T>
            }
        { Expr = Primitive prim }

    member __.ReturnFrom (f : RevAsync<'T>) = f

    member __.Bind(f : RevAsync<'T>, cont: 'T -> RevAsync<'U>): RevAsync<'U> =
        { Expr = Bind(f.Expr, lift cont) }

    member __.Combine(f : RevAsync<unit>, g : RevAsync<'T>) : RevAsync<'T> =
        { Expr = Combine(f.Expr, g.Expr) }

    member __.TryWith(tryBlock: RevAsync<'T>, withBlock: exn -> RevAsync<'T>): RevAsync<'T> =
        { Expr = TryWith(tryBlock.Expr, lift withBlock) }

    member __.TryFinally(tryBlock: RevAsync<'T>, finallyF: unit -> unit): RevAsync<'T> =
        { Expr = TryFinally(tryBlock.Expr, finallyF) }

    member __.For(inputs : 'T seq, bodyF : 'T -> RevAsync<unit>) : RevAsync<unit> =
        let expr = DoSequential(inputs |> Seq.map (lift bodyF) |> LazyList.ofSeq)
        { Expr = expr }

    member __.While(pred : unit -> bool, bodyF : RevAsync<unit>) : RevAsync<unit> =
        let expr = DoSequential(seq { while pred () do yield bodyF.Expr } |> LazyList.ofSeq)
        { Expr = expr }

    member r.Zero() = r.Return()

    member r.Using<'T, 'U when 'T :> IDisposable>(v : 'T, cont : 'T -> RevAsync<'U>) : RevAsync<'U> =
        { Expr = BindDisposable(v :> IDisposable, typeof<'T>, lift cont) }

// a few exceptions

exception CompensationFault of exn
exception FinalizationFault of exn
// indicates severe implementation error at the trampoline; recovery workflows are to be run manually
exception ReversibleInternalFault of exn * Reversal * Reversal  // compensation * finalization

module private InternalImpls =
    // trampoline execution logic ; should be exception safe
    let internal evalTrampoline (ct : CancellationToken) (reversible: RevAsync<'T>) : Async<ReversibleResult<'T>> =
        // reversible bind
        let append item workflow =
            match workflow with
            | RAtom _ | RParallel _ -> RSequential([item ; workflow])
            | RSequential items -> RSequential(item :: items)

        let catch f t = try Choice1Of2 (f t) with e -> Choice2Of2 e

        let inline cancel (cts : CancellationTokenSource) = cts.Cancel()

        let inline isCancelled (cts : CancellationTokenSource) = cts.IsCancellationRequested

        let rec eval ((cts, compensation, finalization) as state) code =
            async {
                // check for external cancellation first
                if isCancelled cts then return TCancelled, compensation, finalization else

                match code with
                // return evaluated result
                | Value(o,t) :: [] -> return TValue(o,t), compensation, finalization
                | Exception e :: [] ->
                    do cancel cts
                    return TException e, compensation, finalization

                // Bind
                | Bind(f, g) :: rest -> 
                    return! eval state (f :: Continuation g :: rest)
                | BindDisposable(d, t, f) :: rest ->
                    let disposal = RAtom <| async { do d.Dispose() }
                    return! eval (cts, compensation, append disposal finalization) (Value(d, t) :: Continuation f :: rest)
                | Value(v,_) :: Continuation f :: rest ->
                    match catch f v with
                    | Choice1Of2 expr -> return! eval state (expr :: rest)
                    | Choice2Of2 e -> return! eval state (Exception e :: rest)

                // Delay
                | Delay f :: rest ->
                    match catch f () with
                    | Choice1Of2 expr -> return! eval state (expr :: rest)
                    | Choice2Of2 e -> return! eval state (Exception e :: rest)

                // Combine
                | Combine(f, g) :: rest -> return! eval state (f :: Combinable g :: rest)
                | Value _ :: Combinable g :: rest -> return! eval state (g :: rest)

                // Sequentials
                | DoSequential(LazyList.Cons(hd, tl)) :: rest ->
                    let unfolded = Combine(hd, DoSequential tl)
                    return! eval state (unfolded :: rest)
                | DoSequential LazyList.Nil :: rest ->
                    let vunit = Value((), typeof<unit>)
                    return! eval state (vunit :: rest)

                // Try/With
                | TryWith(tryBlock, withF) :: rest ->
                    return! eval state (tryBlock :: WithBlock withF :: rest)
                | Value _ as v :: WithBlock _ :: rest -> return! eval state (v :: rest)
                | Exception e :: WithBlock h :: rest ->
                    match catch h e with
                    | Choice1Of2 expr -> return! eval state (expr :: rest)
                    | Choice2Of2 e -> return! eval state (Exception e :: rest)

                // Try/Finally
                | TryFinally(tryBlock, finalizationF) :: rest ->
                    return! eval state (tryBlock :: FinallyBlock finalizationF :: rest)
                | (Value _ | Exception _) as r :: (FinallyBlock _ as f) :: rest -> return! eval state (f :: r :: rest)
                | FinallyBlock f :: rest ->
                    match catch f () with
                    | Choice1Of2 _ -> return! eval state rest
                    | Choice2Of2 e -> return! eval state (Exception e :: rest)

                // primitive handling
                | Primitive { Computation = computation ; Type = t } :: rest ->
                    let! result = Async.Catch computation

                    match result with
                    | Choice1Of2 (value, comp, final) ->
                        let state' = cts, append (RAtom comp) compensation, append (RAtom final) finalization
                        return! eval state' (Value(value, t) :: rest)
                    | Choice2Of2 e -> return! eval state (Exception e :: rest)

                // Parallel workflows
                | RunParallel (t, children) :: rest ->
                    use cts' = CancellationTokenSource.CreateLinkedTokenSource [| cts.Token |]

                    let wrap child = eval (cts', RSequential [], RSequential []) [child]
                    let! result = children |> Seq.map wrap |> Async.Parallel

                    let results, compensations, finalizations = result |> Array.toList |> List.unzip3
                    let compensation' = append (RParallel compensations) compensation
                    let finalization' = append (RParallel finalizations) finalization

                    let map = 
                        results 
                        |> Seq.groupBy (function TValue _ -> 0 | TException _ -> 1 | TFault _ -> 2 | TCancelled _ -> 3)
                        |> Seq.map (fun (i,s) -> (i, Seq.toList s))
                        |> Map.ofSeq

                    let lookup i = defaultArg (map.TryFind i) []

                    let values = lookup 0
                    let exns = lookup 1
                    let faults = lookup 2
                    let cancelled = lookup 3

                    if faults.Length > 0 then
                        do cancel cts
                        return faults.[0], compensation', finalization'
                    elif exns.Length > 0 then
                        let e = match exns.[0] with TException e -> e | _ -> failwith "impossible."
                        return! eval (cts,compensation',finalization') (Exception e :: rest)
                    elif cancelled.Length > 0 then
                        return
                            if isCancelled cts then TCancelled, compensation', finalization'
                            else TFault(new Exception("Reversible parallel inconsistency.")), compensation', finalization'
                    else
                        // computation successful, create result array
                        let result =
                            try
                                let objValues = 
                                    values 
                                    |> Seq.map (function TValue(o,_) -> o | _ -> failwith "impossible.") 
                                    |> Array.ofSeq
                                let resultArr = Array.CreateInstance(t, objValues.Length)
                                do Array.Copy(objValues, resultArr, objValues.Length)
                                Choice1Of2 (Value(resultArr, resultArr.GetType()))
                            with e -> Choice2Of2 (TFault(new Exception("Reversible parallel inconsistency", e)))

                        match result with
                        | Choice1Of2 value -> return! eval (cts,compensation',finalization') (value :: rest)
                        | Choice2Of2 fault -> return fault, compensation', finalization'

                // roll exceptions to the top of the stack
                | Exception _ as e :: _ :: rest -> return! eval state (e :: rest)
                // handle corrupt interpreter state
                | stack -> 
                    do cancel cts
                    return TFault(new Exception(sprintf "Trampoline dump: %A" stack)), compensation, finalization
            }

        async {
            let cts = CancellationTokenSource.CreateLinkedTokenSource [| ct |]
            let! result, compensation, finalization = eval (cts, RSequential [], RSequential []) [reversible.Expr]

            match result with
            | TCancelled ->
                assert ct.IsCancellationRequested
                return Cancelled(compensation, finalization)
            | TException e -> return Failure(e, compensation, finalization)
            | TFault e -> return Fault(e, compensation, finalization)
            | TValue(o,_) ->
                try let v = o :?> 'T in return Success(v, finalization)
                with e -> return Fault(e, compensation, finalization)
        }

    // execute recovery workflow
    let internal evalRecovery runSeq wrap (r : Reversal) : Async<unit> =
        let rec eval r = async {
            match r with
            | RAtom r -> return! r
            | RSequential rs -> for r in rs do do! eval r
            | RParallel rs when runSeq -> for r in rs do do! eval r
            | RParallel rs -> do! Seq.map eval rs |> Async.Parallel |> Async.Ignore
        }

        async { try return! eval r with e -> return! Async.Raise (wrap e) }

    let internal execRecovery isRecoverySequential recoverOnCancellation (r : ReversibleResult<'T>) : Async<'T> =
        async {
            match r with
            | Success(result, finalization) ->
                do! evalRecovery isRecoverySequential FinalizationFault finalization
                return result
            | Failure(e, compensation, finalization) ->
                do! evalRecovery isRecoverySequential CompensationFault compensation
                do! evalRecovery isRecoverySequential FinalizationFault finalization
                return! Async.Raise e
            | Cancelled(compensation, finalization) ->
                if recoverOnCancellation then
                    do! evalRecovery isRecoverySequential CompensationFault compensation
                    do! evalRecovery isRecoverySequential FinalizationFault finalization

                return! Async.Raise(new OperationCanceledException())
            | Fault(e, compensation, finalization) ->
                return! Async.Raise <| ReversibleInternalFault(e, compensation, finalization)
        }

    let internal execTrampoline isRecoverySequential recoverOnCancellation ct (r : RevAsync<'T>) : Async<'T> =
        async {
            let! result = evalTrampoline ct r

            return! execRecovery isRecoverySequential recoverOnCancellation result
        }

    let internal exec isRecoverySequential recoverOnCancellation (ct: CancellationToken option) (r : RevAsync<'T>) =
        Async.IsolateCancellation((fun ct' -> execTrampoline isRecoverySequential recoverOnCancellation ct' r), ?cancellationToken = ct)
        //execTrampoline isRecoverySequential recoverOnCancellation Async.DefaultCancellationToken r

    let internal toReversible (input : Async<'T * Async<unit> * Async<unit>>) : RevAsync<'T> =
        let prim = { Computation = async { let! r,x,y = input in return r :> obj, x, y } ; Type = typeof<'T> }
        { Expr = Primitive prim }

    let internal toParallel (inputs : RevAsync<'T> seq) : RevAsync<'T []> =
        { Expr = RunParallel(typeof<'T>, inputs |> Seq.map (fun r -> r.Expr)) }


//
//  Public API
//

open InternalImpls

let revasync = new RevAsyncBuilder()

let (|ReversibleFault|_|) = function
    | CompensationFault e -> Some e
    | FinalizationFault e -> Some e
    | ReversibleInternalFault _ as e -> Some e
    | _ -> None

[<RequireQualifiedAccess>]
module Async =
    /// polymorphic Async.Zero with ignore semantics
    let zero _ = async.Zero()

type RevAsync =
    // builder section
    static member FromPrimitiveAsync(computation : Async<'T * _ * _>) : RevAsync<'T> = toReversible computation

    static member FromAsync(computation : Async<'T>, ?recovery : 'T -> Async<unit>, ?finalization : 'T -> Async<unit>) : RevAsync<'T> =
        let recoveryF = defaultArg recovery Async.zero
        let finalizationF = defaultArg finalization Async.zero
        RevAsync.FromComponents(computation, recoveryF, finalizationF)

    static member FromComponents(computation : Async<'T>, recoveryF : 'T -> Async<unit>, finalizationF: 'T -> Async<unit>) : RevAsync<'T> =
        RevAsync.FromPrimitiveAsync <| async { let! r = computation in return r, recoveryF r, finalizationF r }

    static member FromComponents(computation : Async<'T>, recovery : Async<unit>, finalization : Async<unit>) : RevAsync<'T> =
        RevAsync.FromPrimitiveAsync <| async { let! r = computation in return r, recovery, finalization }

    static member FromComponents(computation : Async<'T * 'I>, recoveryF : 'I -> Async<unit>, finalizationF : 'I -> Async<unit>) : RevAsync<'T> =
        RevAsync.FromPrimitiveAsync <| async { let! r, i = computation in return r, recoveryF i, finalizationF i }

    static member FromComponents(computation : unit -> 'T * 'I, recoveryF : 'I -> unit, finalizationF : 'I -> unit) : RevAsync<'T> =
        async {
            let result, innerState = computation ()

            return result, async { recoveryF innerState }, async { finalizationF innerState }
        } |> RevAsync.FromPrimitiveAsync

    static member Parallel(computations : #seq<RevAsync<'T>>) = toParallel computations

    static member Ignore(computation : RevAsync<'T>) = revasync { let! _ = computation in return () }

    static member Raise(e : #exn) : RevAsync<'T> = RevAsync.FromAsync <| Async.Raise e

    // execution section

    static member Isolate(computation : RevAsync<'T>) = computation |> RevAsync.ToAsyncWithRecovery |> RevAsync.FromAsync

    static member ToAsyncWithRecovery(computation : RevAsync<'T>, ?recoverSequentially, ?recoverOnCancellation, ?cancellationToken : CancellationToken) : Async<'T> =
        let recoverSequentially = defaultArg recoverSequentially false
        let recoverOnCancellation = defaultArg recoverOnCancellation true
        exec recoverSequentially recoverOnCancellation cancellationToken computation

    static member RunWithRecovery(computation : RevAsync<'T>, ?recoverSequentially, ?timeout, ?cancellationToken : CancellationToken) : 'T =
        let wf = RevAsync.ToAsyncWithRecovery(computation, ?recoverSequentially = recoverSequentially, ?cancellationToken = cancellationToken)
        Async.RunSynchronously(wf, ?timeout = timeout)

    static member ToAsyncNoRecovery(computation : RevAsync<'T>, ?cancellationToken : CancellationToken) : Async<ReversibleResult<'T>> =
        Async.IsolateCancellation((fun ct -> evalTrampoline ct computation), ?cancellationToken = cancellationToken)

    static member RunNoRecovery(computation : RevAsync<'T>, ?cancellationToken, ?timeout) : ReversibleResult<'T> =
        let wf = RevAsync.ToAsyncNoRecovery(computation, ?cancellationToken = cancellationToken)
        Async.RunSynchronously(wf, ?timeout = timeout)

    static member RecoverAsync (result : ReversibleResult<'T>, ?runSequentially, ?recoverOnCancellation) : Async<'T> =
        let runSequentially = defaultArg runSequentially false
        let recoverOnCancellation = defaultArg recoverOnCancellation true
        execRecovery runSequentially recoverOnCancellation result

    static member Recover (result : ReversibleResult<'T>, ?runSequentially, ?recoverOnCancellation, ?timeout) =
        let wf = RevAsync.RecoverAsync(result, ?runSequentially = runSequentially, ?recoverOnCancellation = recoverOnCancellation)
        Async.RunSynchronously(wf, ?timeout = timeout)


    static member TryRun (computation : RevAsync<'T>, ?runSequentially, ?timeout,
                                                    ?cancellationToken, ?recoverOnCancellation) =
        let recoverOnCancellation = defaultArg recoverOnCancellation true

        let result = RevAsync.RunNoRecovery(computation, ?cancellationToken = cancellationToken, 
                                                                                ?timeout = timeout)
        let mkRecoveryOperation () = async {
            let! r = Async.Catch <| RevAsync.RecoverAsync(result, ?runSequentially = runSequentially, 
                                                            recoverOnCancellation = recoverOnCancellation)
            match r with
            | Choice1Of2 _ -> return ()
            | Choice2Of2 (ReversibleFault e) -> return! Async.Raise e
            | Choice2Of2 _ -> return ()
        }

        match result with
        | Success _ -> RevAsync.Recover(result, ?runSequentially = runSequentially) |> Choice1Of2
        | Failure(e,_,_)
        | Fault(e,_,_) -> Choice2Of2(e, mkRecoveryOperation ())
        | Cancelled _ ->
            let recovery = 
                if recoverOnCancellation then mkRecoveryOperation()
                else async.Zero()

            Choice2Of2(OperationCanceledException() :> _, recovery)

[<AutoOpen>]
module Utils =
    [<RequireQualifiedAccess>]
    module RevAsync =
        /// postcompose covariant operation
        let map (f : 'T -> 'S) (w : RevAsync<'T>) : RevAsync<'S> =
            revasync { let! r = w in return f r }

        /// lifting of lambdas to revasync funcs
        let lift (f : 'T -> 'S) = fun t -> revasync { return f t }


        /// revasync failwith
        let rafailwith msg = RevAsync.Raise(System.Exception msg) : RevAsync<'T>
        /// revasync failwithf
        let rafailwithf fmt = Printf.ksprintf rafailwith fmt : RevAsync<'T>

module List =
    let rec foldRevAsync (foldF: 'U -> 'T -> RevAsync<'U>) (state: 'U) (items: 'T list): RevAsync<'U> =
        revasync {
            match items with
            | [] -> return state
            | item::rest ->
                let! nextState = foldF state item
                return! foldRevAsync foldF nextState rest
        }

    let foldBackRevAsync (foldF: 'T -> 'U -> RevAsync<'U>) (items: 'T list) (state: 'U): RevAsync<'U> =
        let rec loop is k = revasync {
            match is with
            | [] -> return! k state
            | h::t -> return! loop t (fun acc -> revasync { let! acc' = foldF h acc in return! k acc' })
        }

        loop items revasync.Return

    let mapRevAsync (mapF: 'T -> RevAsync<'U>) (items: 'T list): RevAsync<'U list> =
        foldRevAsync (fun is i -> revasync { let! i' = mapF i in return i'::is }) [] items

    let chooseRevAsync (choiceF: 'T -> RevAsync<'U option>) (items: 'T list): RevAsync<'U list> =
        foldRevAsync (fun is i -> revasync { let! r = choiceF i in return match r with Some i' -> i'::is | _ -> is }) [] items
