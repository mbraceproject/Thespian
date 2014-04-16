namespace Nessos.Thespian.Serialization

    open System
    open System.IO
    open System.IO.Compression
    open System.Runtime.Serialization
    open System.Runtime.Serialization.Formatters.Binary
    open System.Collections.Generic
    
    open Nessos.Thespian

    type BinaryFormatterMessageSerializer(?compressSerialization : bool) =
        let compress = defaultArg compressSerialization true

        interface IMessageSerializer with
            member __.Name = "format.binary"

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
        

    type SerializerRegistry private () =
        static let defaultSerializerName = String.Empty
        static let originalDefaultSerializer = new BinaryFormatterMessageSerializer() :> IMessageSerializer
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
