namespace Nessos.Thespian.Serialization

    open System
    open System.IO
    open System.IO.Compression
    open System.Runtime.Serialization
    open System.Runtime.Serialization.Formatters.Binary
    open System.Collections.Generic
    
    open Nessos.FsPickler

    open Nessos.Thespian

    type IMessageSerializer =
        abstract Name: string

        abstract Serialize<'T> : 'T * ?context:StreamingContext -> byte []
        abstract Deserialize<'T> : data:byte[] * ?context:StreamingContext -> 'T

    type BinaryFormatterMessageSerializer(?compressSerialization : bool) =
        let compress = defaultArg compressSerialization true

        static let getFormatter (context : StreamingContext option) =
            match context with
            | None -> new BinaryFormatter()
            | Some ctx -> new BinaryFormatter(null, ctx)

        interface IMessageSerializer with
            member __.Name = "FsPickler"

            member __.Serialize<'T> (value : 'T, ?context:StreamingContext) : byte[] =
                use memoryStream = new MemoryStream()
                let formatter = getFormatter context

                if compress then
                    use zipStream = new GZipStream(memoryStream, CompressionMode.Compress)
                    formatter.Serialize(zipStream, value)
                    zipStream.Close()
                else
                    formatter.Serialize(memoryStream, value)
                
                memoryStream.GetBuffer()

            member __.Deserialize<'T> (bytes : byte[], ?context:StreamingContext) : 'T =
                use memoryStream = new MemoryStream(bytes)
                let formatter = getFormatter context
                
                if compress then
                    use zipStream = new GZipStream(memoryStream, CompressionMode.Decompress)
                    formatter.Deserialize(zipStream) :?> 'T
                else
                    formatter.Deserialize(memoryStream) :?> 'T


    type FsPicklerSerializer(?pickler : FsPickler) =
        
        let pickler = 
            match pickler with 
            | None -> new FsPickler()
            | Some p -> p

        interface IMessageSerializer with
            member __.Name = "FsPickler"

            member __.Serialize<'T> (value:'T, ?context) = pickler.Pickle<'T>(value, ?streamingContext = context)
            member __.Deserialize<'T> (data:byte[], ?context) = pickler.UnPickle<'T>(data, ?streamingContext = context)
        

    type SerializerRegistry private () =
        static let defaultSerializerName = String.Empty
        static let originalDefaultSerializer = new FsPicklerSerializer() :> IMessageSerializer
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

    [<AutoOpen>]
    module Default =
        let serializerNameDefaultArg (name: string option) = defaultArg name SerializerRegistry.DefaultName