namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Thespian")>]
[<assembly: AssemblyProductAttribute("Thespian")>]
[<assembly: AssemblyDescriptionAttribute("An F# Actor Framework")>]
[<assembly: AssemblyVersionAttribute("0.1.9")>]
[<assembly: AssemblyFileVersionAttribute("0.1.9")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.1.9"
