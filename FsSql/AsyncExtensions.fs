module FsSql.Async

open System
open System.Collections.Generic
open System.Data
open System.Data.SqlClient
open System.Xml

type SqlCommand with
    member cmd.AsyncExecNonQuery() =
        let abegin (a,b) = cmd.BeginExecuteNonQuery(a,b)
        let aend = cmd.EndExecuteNonQuery
        let acancel = cmd.Cancel
        Async.FromBeginEnd(abegin, aend, acancel)

    member cmd.AsyncExecReader() =
        let abegin (a,b) = cmd.BeginExecuteReader(a,b)
        let aend = cmd.EndExecuteReader
        let acancel = cmd.Cancel
        Async.FromBeginEnd(abegin, aend, acancel)

    member cmd.AsyncExecXmlReader() =
        let abegin (a,b) = cmd.BeginExecuteXmlReader(a,b)
        let aend = cmd.EndExecuteXmlReader
        let acancel = cmd.Cancel
        Async.FromBeginEnd(abegin, aend, acancel)

let inline asyncExecNonQuery cmd = 
    let abegin (a,b:obj)= (^a: (member BeginExecuteNonQuery: AsyncCallback*obj -> IAsyncResult) (cmd,a,b))
    let aend a = (^a: (member EndExecuteNonQuery: IAsyncResult -> int) (cmd,a))
    let acancel () = (^a: (member Cancel: unit -> unit) (cmd))
    Async.FromBeginEnd(abegin, aend, acancel)

let inline asyncExecReader cmd =
    let abegin (a,b:obj)= (^a: (member BeginExecuteReader: AsyncCallback*obj -> IAsyncResult) (cmd,a,b))
    let aend a = (^a: (member EndExecuteReader: IAsyncResult -> #IDataReader) (cmd,a))
    let acancel () = (^a: (member Cancel: unit -> unit) (cmd))
    Async.FromBeginEnd(abegin, aend, acancel)

let inline asyncExecXmlReader cmd =
    let abegin (a,b:obj)= (^a: (member BeginExecuteXmlReader: AsyncCallback*obj -> IAsyncResult) (cmd,a,b))
    let aend a = (^a: (member EndExecuteXmlReader: IAsyncResult -> XmlReader) (cmd,a))
    let acancel () = (^a: (member Cancel: unit -> unit) (cmd))
    Async.FromBeginEnd(abegin, aend, acancel)
    
type IAsyncOps = 
    abstract execNonQuery: IDbCommand -> int Async
    abstract execReader: IDbCommand -> IDataReader Async

let AsyncOpsRegistry = Dictionary<Type, IAsyncOps>()

let SqlClientAsyncOps = 
    { new IAsyncOps with
        member x.execNonQuery cmd = asyncExecNonQuery (cmd :?> SqlCommand)
        member x.execReader cmd = 
                    async {
                        let! r = asyncExecReader (cmd :?> SqlCommand)
                        return upcast r
                    } }

let FakeAsyncOps = 
    { new IAsyncOps with
        member x.execNonQuery cmd = 
            let e = Func<_>(cmd.ExecuteNonQuery)
            Async.FromBeginEnd(e.BeginInvoke, e.EndInvoke, cmd.Cancel)
        member x.execReader cmd = 
            let e = Func<_>(cmd.ExecuteReader)
            Async.FromBeginEnd(e.BeginInvoke, e.EndInvoke, cmd.Cancel) }

AsyncOpsRegistry.Add(typeof<SqlCommand>, SqlClientAsyncOps)

let getAsyncOpsForCommand (cmd: #IDbCommand) = 
    match AsyncOpsRegistry.TryGetValue (cmd.GetType()) with
    | false,_ -> FakeAsyncOps
    | true,m -> m

