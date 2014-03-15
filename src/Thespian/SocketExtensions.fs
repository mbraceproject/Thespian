namespace Thespian.Remote

    module SocketExtensions =
        open System
        open System.Net
        open System.Net.Sockets

        type TcpListener with
            member listener.AsyncAcceptTcpClient(): Async<TcpClient> =
                Async.FromBeginEnd(listener.BeginAcceptTcpClient, listener.EndAcceptTcpClient)

        type TcpClient with
            member client.AsyncConnent(endPoint: IPEndPoint): Async<unit> =
                Async.FromBeginEnd(
                    (fun (callback: System.AsyncCallback, state: obj) ->  client.BeginConnect(endPoint.Address, endPoint.Port, callback, state)), 
                    client.EndConnect)


        type Dns with
            static member AsyncGetHostAddresses(hostNameOrAddress: string): Async<IPAddress[]> =
                Async.FromBeginEnd(
                    (fun (callback: System.AsyncCallback, state: obj) -> Dns.BeginGetHostAddresses(hostNameOrAddress, callback, state)),
                    Dns.EndGetHostAddresses)
