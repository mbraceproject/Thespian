module Nessos.Thespian.Serialization

open System
open System.Runtime.Serialization

open Nessos.FsPickler
open Nessos.Thespian
open Nessos.Thespian.Utils.Concurrency

type IMessageSerializer =
    abstract Name: string  
    abstract Serialize<'T> : 'T * ?context:StreamingContext -> byte []
    abstract Deserialize<'T> : data:byte[] * ?context:StreamingContext -> 'T

type FsPicklerMessageSerializer(?pickler : FsPicklerSerializer) =
    let pickler =
        match pickler with 
        | None -> new BinarySerializer() :> FsPicklerSerializer
        | Some p -> p

    member __.Pickler = pickler

    member __.Serialize<'T>(value: 'T, ?context) = pickler.Pickle<'T>(value, ?streamingContext = context)
    member __.Deserialize<'T>(data: byte[], ?context) = pickler.UnPickle<'T>(data, ?streamingContext = context)

    interface IMessageSerializer with
        override __.Name = sprintf "FsPickler.%s" pickler.PickleFormat

        override self.Serialize<'T> (value: 'T, ?context) =
            try self.Serialize<'T>(value, ?context = context)
            with e -> raise <| new ThespianSerializationException(sprintf "Failed to serialize value of type %A" typeof<'T>.Name, SerializationOperation.Serialization, e)
        override self.Deserialize<'T>(data: byte[], ?context) =
            try self.Deserialize<'T>(data, ?context = context)
            with e -> raise <| new ThespianSerializationException(sprintf "Failed to deserialize value with expected type %A" typeof<'T>.Name, SerializationOperation.Deserialization, e)


let mutable defaultSerializer = new FsPicklerMessageSerializer() :> IMessageSerializer

        
[<Obsolete("use defaultSerializer value instead.")>]
type SerializerRegistry private () =
    static let defaultSerializerName = String.Empty
    static let originalDefaultSerializer = new FsPicklerMessageSerializer() :> IMessageSerializer
    static let serializerMap = Atom.atom Map.empty<string, IMessageSerializer>
    static let init () =
        serializerMap.Swap(fun _ -> 
            let map = Map.empty
            let map = Map.add defaultSerializerName originalDefaultSerializer map
            let map = Map.add originalDefaultSerializer.Name originalDefaultSerializer map
            map)

    static do init ()

    static member Register(serializer: IMessageSerializer, ?setAsDefault: bool) =
        serializerMap.Swap(fun m ->
            let m = Map.add serializer.Name serializer m
            if defaultArg setAsDefault false then
                Map.add defaultSerializerName serializer m
            else m)

    static member DefaultName = serializerMap.Value.[defaultSerializerName].Name
    static member GetDefaultSerializer() = serializerMap.Value.[defaultSerializerName]

    static member SetDefault(name: string) =
        serializerMap.Swap(fun m ->
            match m.TryFind name with
            | Some serializer -> Map.add defaultSerializerName serializer m
            | None -> invalidArg "name" "No such serializer registered.")

    static member Resolve(name: string) = serializerMap.Value.[name]
    static member IsRegistered(name: string) = serializerMap.Value.ContainsKey name
    static member Clear() = init ()
