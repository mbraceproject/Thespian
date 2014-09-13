module internal Nessos.Thespian.IOExtensions

open System.IO

type BinaryWriter with
    member writer.WriteByteArray(bytes: byte[]) =
        writer.Write(bytes.Length)
        writer.Write(bytes)

type BinaryReader with
    member reader.ReadByteArray(): byte[] =
        let length = reader.ReadInt32()
        reader.ReadBytes(length)
