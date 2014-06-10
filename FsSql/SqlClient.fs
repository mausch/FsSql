module FsSql.SqlClient

open System
open System.Data
open System.Data.SqlClient
open Microsoft.FSharp.Reflection
open Sql

type TableValuedParameter = { 
    Name : string
    TableValueType : Type
    TableValues : seq<obj> 
} with 
    static member make<'a>(name, (values : seq<'a>)) = 
        { Name = name
          TableValueType = typeof<'a>
          TableValues = values |> Seq.map (fun v -> v :> obj) }

type SqlClientParameter = | Standard of Parameter | TableValued of TableValuedParameter

let internal makeStructuredTableParam tvp =
    let fields = FSharpType.GetRecordFields tvp.TableValueType
    let dt = new DataTable()
    // add cols to DataTable
    for f in fields do
        if f.PropertyType.IsPrimitive || f.PropertyType = typeof<string> then
            dt.Columns.Add(f.Name, f.PropertyType) |> ignore
        else failwithf "Only primitive types and strings allowed for table valued params. Field %s was type %A" f.Name f.PropertyType
    // add rows to DataTable
    for v in tvp.TableValues do
        let rowVals = fields |> Array.map (fun f -> f.GetValue(v, null))
        dt.Rows.Add(rowVals) |> ignore
    new SqlParameter(ParameterName = tvp.Name, SqlDbType = SqlDbType.Structured, Value = dt, Direction = ParameterDirection.Input)

let internal addSqlParameter (cmd : SqlCommand) sqlParam =
    match sqlParam with
    | Standard p -> addParameter cmd p
    | TableValued tvp -> 
        let p = makeStructuredTableParam tvp
        cmd.Parameters.Add(p) |> ignore

let internal prepareSqlClientCommand (connection: SqlConnection) (tx: SqlTransaction option) (sql: string) (cmdType: CommandType) (parameters: seq<SqlClientParameter>) =
    let cmd = connection.CreateCommand()
    cmd.CommandText <- sql
    cmd.Transaction <- Option.getOrDefault tx
    cmd.CommandType <- cmdType
    parameters |> Seq.iter (addSqlParameter cmd)
    cmd

let internal execSqlReaderInternal (exec: SqlCommand -> (unit -> unit) -> 'a) cmdType (cmgr: ConnectionManager) (sql: string) (parameters: seq<SqlClientParameter>) =
    let conn = cmgr.create() 
    let sqlConn = conn :?> SqlConnection
    let sqlTx = match cmgr.tx with | Some tx -> Some (tx :?> SqlTransaction) | None -> None
    let cmd = prepareSqlClientCommand sqlConn sqlTx sql cmdType parameters
    let dispose() = 
        cmd.Dispose()
        cmgr.dispose conn
    exec cmd dispose

/// Executes a stored procedure and returns a data reader
let execSPReader connMgr = execSqlReaderInternal execReaderWrap CommandType.StoredProcedure connMgr