namespace Nessos.Thespian.Utils

open System.Threading

/// Thread-safe value container with optimistic update semantics
type Atom<'T when 'T : not struct>(value : 'T) =
    let refCell = ref value

    let rec swap f = 
        let currentValue = !refCell
        let result = Interlocked.CompareExchange<'T>(refCell, f currentValue, currentValue)
        if obj.ReferenceEquals(result, currentValue) then ()
        else Thread.SpinWait 20; swap f

    let transact f =
        let result = ref Unchecked.defaultof<_>
        let f' t = let t',r = f t in result := r ; t'
        swap f' ; result.Value

    /// Get Current Value
    member __.Value with get() : 'T = !refCell

    /// <summary>
    /// Atomically updates the container.
    /// </summary>
    /// <param name="updateF">updater function.</param>
    member __.Swap (f : 'T -> 'T) : unit = swap f

    /// <summary>
    /// Perform atomic transaction on container.
    /// </summary>
    /// <param name="transactionF">transaction function.</param>
    member __.Transact(f : 'T -> 'T * 'R) : 'R = transact f

    /// <summary>
    /// Force a new value on container.
    /// </summary>
    /// <param name="value">value to be set.</param>
    member __.Force (value : 'T) = refCell := value

/// Atom utilities module
[<RequireQualifiedAccess>]
module Atom =

    /// <summary>
    /// Initialize a new atomic container with given value.
    /// </summary>
    /// <param name="value">Initial value.</param>
    let create<'T when 'T : not struct> value = new Atom<'T>(value)
    
    /// <summary>
    /// Atomically updates the container with given function.
    /// </summary>
    /// <param name="atom">Atom to be updated.</param>
    /// <param name="updateF">Updater function.</param>
    let swap (atom : Atom<'T>) f = atom.Swap f

    /// <summary>
    /// Perform atomic transaction on container.
    /// </summary>
    /// <param name="atom">Atom to perform transaction on.</param>
    /// <param name="transactF">Transaction function.</param>
    let transact (atom : Atom<'T>) f : 'R = atom.Transact f

    /// <summary>
    ///     Force value on given atom.
    /// </summary>
    /// <param name="atom">Atom to be updated.</param>
    /// <param name="value">Value to be set.</param>
    let force (atom : Atom<'T>) t = atom.Force t


/// <summary>
///     Thread-safe latch
/// </summary>
type Latch() =
    [<VolatileField>]
    let mutable switch = 0

    /// <summary>
    ///     Trigger the latch; returns true iff call was successful.
    /// </summary>
    member __.Trigger() : bool = Interlocked.CompareExchange(&switch, 1, 0) = 0

/// <summary>
///     Thread safe counter implementation
/// </summary>
type ConcurrentCounter (?start : int64) =
    [<VolatileField>]
    let mutable counter = defaultArg start 0L

    /// <summary>
    ///     Increment the counter
    /// </summary>
    member __.Increment () = Interlocked.Increment &counter

    /// <summary>
    ///     Current counter value
    /// </summary>
    member __.Value = counter

/// <summary>
///     Thread-safe countdown latch.
/// </summary>
type CountdownLatch() =
    [<VolatileField>]
    let mutable counter = 0

    /// Set the latch
    member __.Increment () = Interlocked.Increment &counter

    /// Reset the latch
    member __.Decrement () = Interlocked.Decrement &counter

    /// Spin-wait until the latch is reset
    member __.WaitToZero(): unit = while (Interlocked.CompareExchange(&counter, 0, 0) <> 0) do Thread.SpinWait 20