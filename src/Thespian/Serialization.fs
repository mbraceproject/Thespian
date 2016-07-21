module Nessos.Thespian.Serialization

    open System
    open System.Runtime.Serialization

    open MBrace.FsPickler
    open Nessos.Thespian

    /// Abstract message serialization interface used by Thespian
    type IMessageSerializer =
        /// Serialization identifier
        abstract Name: string  

        /// <summary>
        ///     Serialize a value to byte array.
        /// </summary>
        /// <param name="T">serialized value.</param>
        /// <param name="context">Streaming context.</param>
        abstract Serialize<'T> : 'T * ?context:StreamingContext -> byte []

        /// <summary>
        ///     Deserialize a value of byte array.
        /// </summary>
        /// <param name="data">pickled value.</param>
        /// <param name="context">Streaming context.</param>
        abstract Deserialize<'T> : data:byte[] * ?context:StreamingContext -> 'T

    /// <summary>
    ///     FsPickler Implementation for IMessageSerializer.
    /// </summary>
    type FsPicklerMessageSerializer(?serializer : FsPicklerSerializer) =

        let serializer =
            match serializer with 
            | None -> new BinarySerializer() :> FsPicklerSerializer
            | Some p -> p

        /// <summary>
        ///     Direct access to the FsPickler instance.
        /// </summary>
        member __.Pickler = serializer

        /// <summary>
        ///     Serialize a value to byte array.
        /// </summary>
        /// <param name="T">serialized value.</param>
        /// <param name="context">Streaming context.</param>
        member __.Serialize<'T>(value: 'T, ?context) = serializer.Pickle<'T>(value, ?streamingContext = context)

        /// <summary>
        ///     Deserialize a value of byte array.
        /// </summary>
        /// <param name="data">pickled value.</param>
        /// <param name="context">Streaming context.</param>
        member __.Deserialize<'T>(data: byte[], ?context) = serializer.UnPickle<'T>(data, ?streamingContext = context)

        interface IMessageSerializer with
            override __.Name = sprintf "FsPickler.%s" serializer.PickleFormat

            override self.Serialize<'T> (value: 'T, ?context) =
                try self.Serialize<'T>(value, ?context = context)
                with e -> raise <| new ThespianSerializationException(sprintf "Failed to serialize value of type %A" typeof<'T>.Name, SerializationOperation.Serialization, e)
            override self.Deserialize<'T>(data: byte[], ?context) =
                try self.Deserialize<'T>(data, ?context = context)
                with e -> raise <| new ThespianSerializationException(sprintf "Failed to deserialize value with expected type %A" typeof<'T>.Name, SerializationOperation.Deserialization, e)


    /// Gets or sets the default global serializer used by Thespian
    let mutable defaultSerializer : IMessageSerializer = new FsPicklerMessageSerializer() :> _