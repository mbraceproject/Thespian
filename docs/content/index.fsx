(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin"

#r "FsPickler.dll"
#r "Thespian.dll"

(**
# Thespian [draft]

A distributed actors library based on F# MailboxProcessor.

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      Thespian can be <a href="https://nuget.org/packages/Thespian">installed from NuGet</a>:
      <pre>PM> Install-Package Thespian -Pre</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

## Example

This example demonstrates using a function defined in this sample library.

*)

open Nessos.Thespian

type Msg = Msg of IReplyChannel<int> * int

let behavior state (Msg (rc,v)) = async {
    printfn "Received %d" v
    rc.Reply <| Value state
    return (state + v)
}

let actor =
    behavior
    |> Behavior.stateful 0 
    |> Actor.bind
    |> Actor.start

let post v = actor.Ref <!= fun ch -> Msg(ch, v)

post 42

(**

Samples & documentation
-----------------------

 * [Tutorial](tutorial.html) contains a further explanation of this sample library.

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library. This includes additional brief samples on using most of the
   functions.
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read [library design notes][readme] to understand how it works.

The library is available under the Apache license.
For more information see the [License file][license] in the GitHub repository. 

  [content]: https://github.com/nessos/Thespian/tree/master/docs/content
  [gh]: https://github.com/nessos/Thespian
  [issues]: https://github.com/nessos/Thespian/issues
  [readme]: https://github.com/nessos/Thespian/blob/master/README.md
  [license]: https://github.com/nessos/Thespian/blob/master/LICENSE.txt
*)
