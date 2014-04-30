namespace Nessos.Thespian.Cluster


    [<AutoOpen>]
    module internal Utils =

        /// stackless raise operator
        let inline raise (e: System.Exception) = (# "throw" e : 'T #)