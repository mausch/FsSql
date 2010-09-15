[<AutoOpen>]
module FsSql.AsyncExtensions

open System.Data.SqlClient

type SqlCommand with
    member cmd.AsyncExecNonQuery() =
        async {
            let abegin (a,b) = cmd.BeginExecuteNonQuery(a,b)
            let aend = cmd.EndExecuteNonQuery
            let acancel = cmd.Cancel
            return! Async.FromBeginEnd(abegin, aend, acancel)
        }

    member cmd.AsyncExecReader() =
        async {
            let abegin (a,b) = cmd.BeginExecuteReader(a,b)
            let aend = cmd.EndExecuteReader
            let acancel = cmd.Cancel
            return! Async.FromBeginEnd(abegin, aend, acancel)
        }

    member cmd.AsyncExecXmlReader() =
        async {
            let abegin (a,b) = cmd.BeginExecuteXmlReader(a,b)
            let aend = cmd.EndExecuteXmlReader
            let acancel = cmd.Cancel
            return! Async.FromBeginEnd(abegin, aend, acancel)
        }