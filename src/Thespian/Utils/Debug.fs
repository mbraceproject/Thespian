module Nessos.Thespian.Debug

open System
open System.Diagnostics

let inline private debugWriteLine (s: string) = Debug.WriteLine s
let inline private debugWriteLineWithCategory (category: string) (s: string) = Debug.WriteLine(s, category)

let inline writelf (format: Printf.StringFormat<'T, unit>) = ignore() //Printf.ksprintf debugWriteLine format

let inline writelfc (category: string) (format: Printf.StringFormat<'T, unit>) =
    ignore()
    //Printf.ksprintf (debugWriteLineWithCategory category) format


