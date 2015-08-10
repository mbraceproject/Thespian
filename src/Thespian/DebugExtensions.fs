namespace Nessos.Thespian.DebugUtils

open System
open System.Collections.Generic
open System.Runtime

open Microsoft.FSharp.Control

open Nessos.Thespian
open Nessos.Thespian.Serialization
open Nessos.Thespian.Remote.TcpProtocol.Unidirectional

type private DebugMSG = Enqueue of Type * isReply:bool * isSerialization:bool | Show

/// An IMessageSerializer wrapper that collects data on serialized messages over a given interval.
[<AutoSerializable(false)>]
type DebugSerializer(source : IMessageSerializer, ?dumpInterval : TimeSpan, ?reportThreshold : int, ?logger : string -> unit) =
    let dict = new Dictionary<Type * bool * bool, int64>()
    let logger = defaultArg logger Console.WriteLine
    let logF fmt = Printf.ksprintf logger fmt
    let dumpInterval =
        match dumpInterval with
        | Some s -> int s.TotalMilliseconds
        | None -> 5000

    let reportThreshold = defaultArg reportThreshold 10

    let rec loop (inbox : MailboxProcessor<DebugMSG>) = async {
        let! msg = inbox.Receive()
        match msg with
        | Show ->
            if dict.Count = 0 then () else
            logF "THESPIAN SERIALIZATION STATUS REPORT %O:" DateTime.Now
            for KeyValue((t,isReply,isSerialization), count) in dict |> Seq.sortBy(fun kv -> - kv.Value) do
                if count >= int64 reportThreshold then
                    let verb = if isSerialization then "serialized" else "deserialized"
                    let reply = if isReply then " [REPLY]" else ""
                    logF "%O was %s %d times%s." t verb count reply

            dict.Clear()

        | Enqueue (t, isReply, isSerialization) ->
            let k = t, isReply, isSerialization
            let mutable v = 0L
            let count =
                if dict.TryGetValue(k, &v) then v + 1L
                else 0L

            dict.[k] <- count

        return! loop inbox
    }

    let mbox = MailboxProcessor.Start loop

    let rec loop () = async {
        mbox.Post Show
        do! Async.Sleep 5000
        return! loop ()
    }

    let extractType (input:obj) =
        let gt o = if obj.ReferenceEquals(o,null) then typeof<obj> else o.GetType()
        match input with
        | :? ProtocolMessage<obj> as msg ->
            match msg with
            | Request obj -> false, gt obj
            | Response (Value t) -> true, gt t
            | Response (Exn e) -> true, gt e
        | _ -> false, gt input

    do Async.Start(loop ())

    interface IMessageSerializer with
        member x.Name: string = sprintf "DEBUG SERIALIZER [%s]" source.Name

        member x.Serialize(t: 'T, context: Runtime.Serialization.StreamingContext option): byte [] = 
            let isReply, ty = extractType t
            mbox.Post <| Enqueue(ty, isReply, true)
            source.Serialize(t, ?context = context)

        member x.Deserialize(data: byte [], context: Runtime.Serialization.StreamingContext option): 'T = 
            let t = source.Deserialize<'T>(data, ?context = context)
            let isReply, ty = extractType t
            mbox.Post <| Enqueue(ty, isReply, false)
            t