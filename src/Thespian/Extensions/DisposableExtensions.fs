namespace Nessos.Thespian


    module DisposableExtensions = 
        open System
    
        open Nessos.Thespian.Agents

        //let private counter = Agent.start Map.empty<IComparable, int>

        type NestedDisposable<'T when 'T :> IDisposable and 'T : comparison>(resource: 'T) =
            static let counter = Agent.start Map.empty<'T, int>
            let key = resource

            let inc counterMap = 
                match Map.tryFind key counterMap with 
                | Some count -> 
                    Map.add key (count + 1) counterMap
                | None -> Map.add key 1 counterMap

            let dec counterMap =
                match Map.tryFind key counterMap with
                | Some 1 -> resource.Dispose(); Map.remove key counterMap
                | Some count -> Map.add key (count - 1) counterMap
                | None -> counterMap

            do Agent.send inc counter

            member d.Resource = resource

            interface IDisposable with
                member d.Dispose() = Agent.send dec counter

        let useNested (resource: 'T when 'T :> IDisposable and 'T : equality): NestedDisposable<'T> = new NestedDisposable<'T>(resource)


