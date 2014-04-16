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
        abstract Serialize : context:obj * obj -> byte []
        abstract Deserialize : context:obj * byte [] -> obj


    type BinaryFormatterMessageSerializer(?compressSerialization : bool) =
        let compress = defaultArg compressSerialization true

        interface IMessageSerializer with
            member __.Name = "FsPickler"

            member __.Serialize (context: obj, graph : obj) : byte[] =
                let formatter = new BinaryFormatter(null, StreamingContext(StreamingContextStates.All, context))

                use memoryStream = new MemoryStream()

                if compress then
                    use zipStream = new GZipStream(memoryStream, CompressionMode.Compress)
                    formatter.Serialize(zipStream, graph)
                    zipStream.Close()
                else
                    formatter.Serialize(memoryStream, graph)
                
                memoryStream.GetBuffer()

            member __.Deserialize (context: obj, bytes : byte[]) : obj =
                let formatter = new BinaryFormatter(null, StreamingContext(StreamingContextStates.All, context))

                use memoryStream = new MemoryStream(bytes)
                
                if compress then
                    use zipStream = new GZipStream(memoryStream, CompressionMode.Decompress)
                    formatter.Deserialize(zipStream)
                else
                    formatter.Deserialize(memoryStream)


    type FsPicklerSerializer(?pickler : FsPickler) =
        
        let pickler = 
            match pickler with 
            | None -> new FsPickler()
            | Some p -> p

        static let getStreamingContext(ctx:obj) = 
            new StreamingContext(StreamingContextStates.All, ctx)

        interface IMessageSerializer with
            member __.Name = "FsPickler"

            member __.Serialize (context:obj, graph:obj) = pickler.Pickle<obj>(graph, getStreamingContext context)
            member __.Deserialize (context:obj, data:byte[]) = pickler.UnPickle<obj>(data, getStreamingContext context)
        

    type SerializerRegistry private () =
        static let defaultSerializerName = String.Empty
        static let originalDefaultSerializer = new FsPicklerSerializer() :> IMessageSerializer
        static let serializerMap = 
            let map = new Dictionary<string, IMessageSerializer>()
            map.Add(defaultSerializerName, originalDefaultSerializer)
            map.Add(originalDefaultSerializer.Name, originalDefaultSerializer)
            map

        static member Register(serializer: IMessageSerializer, ?setAsDefault: bool) =
            serializerMap.[serializer.Name] <- serializer
            if defaultArg setAsDefault false then
                serializerMap.[defaultSerializerName] <- serializer

        static member DefaultName = serializerMap.[defaultSerializerName].Name
        
        static member GetDefaultSerializer() =
            serializerMap.[defaultSerializerName]

        static member SetDefault(name: string) =
            if serializerMap.ContainsKey name then
                serializerMap.[defaultSerializerName] <- serializerMap.[name]
            else invalidArg "name" "No such serializer registered."

        static member Resolve(name: string) =
            serializerMap.[name]

        static member IsRegistered(name: string) =
            serializerMap.ContainsKey name

        static member Clear() =
            serializerMap.Clear()
            serializerMap.Add(defaultSerializerName, originalDefaultSerializer)
            serializerMap.Add(originalDefaultSerializer.Name, originalDefaultSerializer)

    [<AutoOpen>]
    module Default =
        let serializerNameDefaultArg (name: string option) = defaultArg name SerializerRegistry.DefaultName