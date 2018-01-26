module FsSql.Async

open System
open System.Collections.Generic
open System.Data
open System.Data.SqlClient
open System.Threading
open System.Threading.Tasks
open System.Xml

type SqlCommand with
    member cmd.AsyncExecNonQuery(?cancellationToken) =
        match cancellationToken with
        | Some ct -> cmd.ExecuteNonQueryAsync(ct) |> Async.AwaitTask
        | None    -> cmd.ExecuteNonQueryAsync() |> Async.AwaitTask

    member cmd.AsyncExecReader(?cancellationToken:CancellationToken) : SqlDataReader Async =
        match cancellationToken with
        | Some ct -> cmd.ExecuteReaderAsync(ct) |> Async.AwaitTask
        | None    -> cmd.ExecuteReaderAsync() |> Async.AwaitTask

    member cmd.AsyncExecXmlReader(?cancellationToken) =
        match cancellationToken with
        | Some ct -> cmd.ExecuteXmlReaderAsync(ct) |> Async.AwaitTask
        | None    -> cmd.ExecuteXmlReaderAsync() |> Async.AwaitTask

type IAsyncOps =
    abstract execNonQuery: IDbCommand -> int Async
    abstract execReader: IDbCommand -> IDataReader Async

let AsyncOpsRegistry = Dictionary<Type, IAsyncOps>()

let SqlClientAsyncOps =
    { new IAsyncOps with
        member x.execNonQuery cmd = (cmd :?> SqlCommand).AsyncExecNonQuery()
        member x.execReader cmd =
                    async {
                        let! r = (cmd :?> SqlCommand).AsyncExecReader()
                        return upcast r
                    } }

let FakeAsyncOps =
    { new IAsyncOps with
        member x.execNonQuery cmd =
            async { return cmd.ExecuteNonQuery() }
        member x.execReader cmd =
            async { return cmd.ExecuteReader() }
    }

AsyncOpsRegistry.Add(typeof<SqlCommand>, SqlClientAsyncOps)

let getAsyncOpsForCommand (cmd: #IDbCommand) =
    match AsyncOpsRegistry.TryGetValue (cmd.GetType()) with
    | false,_ -> FakeAsyncOps
    | true,m -> m

