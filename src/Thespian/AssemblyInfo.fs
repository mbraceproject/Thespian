namespace System
open System.Reflection
open System.Runtime.CompilerServices

[<assembly: AssemblyTitleAttribute("Thespian")>]
[<assembly: AssemblyProductAttribute("Thespian")>]
[<assembly: AssemblyDescriptionAttribute("An F# Actor Framework")>]
[<assembly: InternalsVisibleToAttribute("Thespian.Cluster")>]
[<assembly: AssemblyVersionAttribute("0.0.4")>]
[<assembly: AssemblyFileVersionAttribute("0.0.4")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.4"
