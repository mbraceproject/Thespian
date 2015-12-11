namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Thespian.Cluster")>]
[<assembly: AssemblyProductAttribute("Thespian.Cluster")>]
[<assembly: AssemblyDescriptionAttribute("An F# Actor Framework")>]
[<assembly: AssemblyVersionAttribute("0.1.7")>]
[<assembly: AssemblyFileVersionAttribute("0.1.7")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.1.7"
