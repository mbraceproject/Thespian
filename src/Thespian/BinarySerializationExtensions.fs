namespace Thespian.Remote.TcpProtocol

    open System
    open System.IO

    module BinarySerializationExtensions =
        type BinaryWriter with
            member writer.WriteByteArray(bytes: byte[]) =
                writer.Write(bytes.Length)
                writer.Write(bytes)

        type BinaryReader with
            member reader.ReadByteArray(): byte[] =
                let length = reader.ReadInt32()
                reader.ReadBytes(length)
