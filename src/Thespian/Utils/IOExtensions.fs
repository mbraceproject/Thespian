module Nessos.Thespian.Utils.IOExtensions

    open System.IO

    type BinaryWriter with

        /// <summary>
        ///     Writes a length-prefixed byte array to underlying stream.
        /// </summary>
        /// <param name="bytes">Input buffer.</param>
        member writer.WriteByteArray(bytes: byte[]) =
            writer.Write(bytes.Length)
            writer.Write(bytes)

    type BinaryReader with

        /// <summary>
        ///     Reads a length-prefixed by array from underlying stream.
        /// </summary>
        member reader.ReadByteArray(): byte[] =
            let length = reader.ReadInt32()
            reader.ReadBytes(length)
