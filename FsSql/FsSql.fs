﻿module Sql

open System
open System.Collections.Generic
open System.Data
open System.Data.SqlClient
open System.Linq
open System.Text.RegularExpressions
open Microsoft.FSharp.Reflection
open FsSqlImpl
open FsSqlPrelude
open FsSql.AsyncExtensions

/// Encapsulates how to create and dispose a database connection
type ConnectionManager = (unit -> IDbConnection) * (IDbConnection -> unit) * (IDbTransaction option)

/// Creates a <see cref="ConnectionManager"/> with an externally-owned connection
let withConnection (conn: IDbConnection) : ConnectionManager =
    let id = Guid.NewGuid().ToString()
    let create() = 
        logf "creating connection from const %s" id
        conn
    let dispose c = logf "disposing connection (but not really) %s" id
    create,dispose,None

let withTransaction (tx: IDbTransaction): ConnectionManager =
    let id = Guid.NewGuid().ToString()
    let create() = 
        logf "creating connection from const %s" id
        tx.Connection
    let dispose c = logf "disposing connection (but not really) %s" id
    create,dispose,Some tx

/// Creates a <see cref="ConnectionManager"/> with an owned connection
let withNewConnection (create: unit -> IDbConnection) : ConnectionManager = 
    let id = Guid.NewGuid().ToString()
    let create() =
        logf "creating connection %s" id
        create()
    let dispose (c: IDisposable) = 
        c.Dispose()
        logf "disposing connection %s" id
    create,dispose,None

let internal withCreateConnection (create: unit -> IDbConnection) : ConnectionManager = 
    let dispose x = ()
    create,dispose,None

let internal doWithConnection (cmgr: ConnectionManager) f =
    let create,dispose,_ = cmgr
    withResource create dispose f
    
let internal PrintfFormatProc (worker: string * obj list -> 'd)  (query: PrintfFormat<'a, _, _, 'd>) : 'a =
    if not (FSharpType.IsFunction typeof<'a>) then
        let result = worker (query.Value, [])
        unbox result
    else
        let rec getFlattenedFunctionElements (functionType: Type) =
            let domain, range = FSharpType.GetFunctionElements functionType
            if not (FSharpType.IsFunction range)
                then domain::[range]
                else domain::getFlattenedFunctionElements(range)
        let types = getFlattenedFunctionElements typeof<'a>
        let rec makeFunctionType (types: Type list) =
            match types with
            | [x;y] -> FSharpType.MakeFunctionType(x,y)
            | x::xs -> FSharpType.MakeFunctionType(x,makeFunctionType xs)
            | _ -> failwith "shouldn't happen"
        let rec proc (types: Type list) (values: obj list) (a: obj) : obj =
            let id = Guid.NewGuid().ToString()
            let values = a::values
            match types with
            | [x;y] -> 
                let result = worker (query.Value, List.rev values)
                box result
            | x::y::z::xs -> 
                let cont = proc (y::z::xs) values
                let ft = makeFunctionType (y::z::xs)
                let cont = FSharpValue.MakeFunction(ft, cont)
                box cont
            | _ -> failwith "shouldn't happen"
        
        let handler = proc types []
        unbox (FSharpValue.MakeFunction(typeof<'a>, handler))

let internal sqlProcessor (cmgr: ConnectionManager) (withCmd: IDbCommand -> IDbConnection -> 'a) (sql: string, values: obj list) =
    let stripFormatting s =
        let i = ref -1
        let eval (rxMatch: Match) =
            incr i
            sprintf "@p%d" !i
        Regex.Replace(s, "%.", eval)
    let sql = stripFormatting sql
    let sqlProcessor' (conn: IDbConnection) = 
        use cmd = conn.CreateCommand()
        cmd.CommandText <- sql
        let _,_,tx = cmgr
        cmd.Transaction <- 
            match tx with
            | None -> null
            | Some t -> t
        let createParam i (p: obj) =
            let param = cmd.CreateParameter()
            //param.DbType <- DbType.
            param.ParameterName <- sprintf "@p%d" i
            param.Value <- p
            cmd.Parameters.Add param |> ignore
        values |> Seq.iteri createParam
        withCmd cmd conn
    doWithConnection cmgr sqlProcessor'

let internal sqlProcessorToDataReader (cmgr: ConnectionManager) b = 
    let create,dispose,_ = cmgr
    let exec (cmd: IDbCommand) (conn: IDbConnection) = 
        let dispose() = dispose conn
        new DataReaderWrapper(cmd.ExecuteReader(), dispose) :> IDataReader
    sqlProcessor (withCreateConnection create) exec b

let internal sqlProcessorNonQuery a b =
    let exec (cmd: IDbCommand) x = 
        cmd.ExecuteNonQuery()
    sqlProcessor a exec b

/// Executes a printf-formatted query and returns a data reader
let execReaderF connectionFactory a = PrintfFormatProc (sqlProcessorToDataReader connectionFactory) a

/// Executes a printf-formatted SQL statement and returns the number of rows affected
let execNonQueryF connectionFactory a = PrintfFormatProc (sqlProcessorNonQuery connectionFactory) a

/// Represents a parameter to a command
type Parameter = {
    DbType: DbType
    Direction: ParameterDirection
    ParameterName: string
    Value: obj
} with
    static member make(parameterName, value: obj) =
        { DbType = Unchecked.defaultof<DbType>
          Direction = ParameterDirection.Input
          ParameterName = parameterName
          Value = value }

/// Adds a parameter to a command
let addParameter (cmd: #IDbCommand) (p: Parameter) =
    let par = cmd.CreateParameter()
    par.DbType <- p.DbType
    par.Direction <- p.Direction
    par.ParameterName <- p.ParameterName
    par.Value <- 
        match p.Value with
        | null -> box DBNull.Value
        | FSharpValue.OptionType -> optionToDBNull p.Value
        | x -> x
    cmd.Parameters.Add par |> ignore

let createCommand (cmgr: ConnectionManager) = 
    let create,dispose,tx = cmgr
    let conn = create()
    let cmd = conn.CreateCommand()
    cmd.Transaction <- 
        match tx with
        | None -> null
        | Some t -> t
    let dispose () = dispose conn
    new DbCommandWrapper(cmd, dispose) :> IDbCommand

let internal prepareCommand (connection: #IDbConnection) (tx: IDbTransaction option) (sql: string) (cmdType: CommandType) (parameters: #seq<Parameter>) =
    let cmd = connection.CreateCommand()
    cmd.CommandText <- sql
    cmd.Transaction <- 
        match tx with
        | None -> null
        | Some t -> t
    cmd.CommandType <- cmdType
    parameters |> Seq.iter (addParameter cmd)
    cmd

let internal inferParameterDbType (p: string * obj) = 
    Parameter.make(fst p, snd p)

/// Creates a list of parameters
let parameters (p: #seq<string * obj>) = 
    p |> Seq.map inferParameterDbType

let paramsFromDict (p: #IDictionary<string, obj>) =
    p |> Seq.map (|KeyValue|) |> parameters

/// Executes and returns a data reader
let internal execReaderInternal (exec: IDbCommand -> (unit -> unit) -> 'a) cmdType (cmgr: ConnectionManager) (sql: string) (parameters: #seq<Parameter>) =
    let create,dispose,tx = cmgr
    let connection = create()
    let cmd = prepareCommand connection tx sql cmdType parameters
    let dispose() = 
        cmd.Dispose()
        dispose connection
    exec cmd dispose

let internal execReaderWrap (cmd: #IDbCommand) dispose = 
    new DataReaderWrapper(cmd.ExecuteReader(), dispose) :> IDataReader

type AsyncOps = {
    execNonQuery: IDbCommand -> int Async
    execReader: IDbCommand -> IDataReader Async
}

let AsyncOpsRegistry = Dictionary<Type, AsyncOps>()

let SqlClientAsyncOps = {
    execNonQuery = fun cmd -> asyncExecNonQuery (cmd :?> SqlCommand)
    execReader = fun cmd -> 
                    async {
                        let! r = asyncExecReader (cmd :?> SqlCommand)
                        return upcast r
                    }
}

let FakeAsyncOps = {
    execNonQuery = fun cmd -> 
                    let e = Func<_>(cmd.ExecuteNonQuery)
                    Async.FromBeginEnd(e.BeginInvoke, e.EndInvoke, cmd.Cancel)
    execReader = fun cmd ->
                    let e = Func<_>(cmd.ExecuteReader)
                    Async.FromBeginEnd(e.BeginInvoke, e.EndInvoke, cmd.Cancel)
}

AsyncOpsRegistry.Add(typeof<SqlCommand>, SqlClientAsyncOps)

let internal execReaderAsyncWrap (cmd: #IDbCommand) dispose = 
    let execReader = 
        match AsyncOpsRegistry.TryGetValue (cmd.GetType()) with
        | false,_ -> FakeAsyncOps.execReader
        | true,m -> m.execReader
    async {
        let! r = execReader cmd
        return new DataReaderWrapper(r, dispose) :> IDataReader
    }

/// Executes a query and returns a data reader
let execReader connMgr = execReaderInternal execReaderWrap CommandType.Text connMgr

/// Executes a stored procedure and returns a data reader
let execSPReader connMgr = execReaderInternal execReaderWrap CommandType.StoredProcedure connMgr

/// Executes a query and returns a data reader
let asyncExecReader connMgr = execReaderInternal execReaderAsyncWrap CommandType.Text connMgr

/// Executes and returns the number of rows affected
let internal execNonQueryInternal cmdType (cmgr: ConnectionManager) (sql: string) (parameters: #seq<Parameter>) =
    let execNonQuery' connection = 
        let _,_,tx = cmgr
        use cmd = prepareCommand connection tx sql cmdType parameters
        cmd.ExecuteNonQuery()
    doWithConnection cmgr execNonQuery'

/// Executes a SQL statement and returns the number of rows affected
let execNonQuery connMgr = execNonQueryInternal CommandType.Text connMgr

/// Executes a stored procedure and returns the number of rows affected
let execSPNonQuery connMgr = execNonQueryInternal CommandType.StoredProcedure connMgr

/// Wraps a function in a transaction with the specified <see cref="IsolationLevel"/>
let transactionalWithIsolation (isolation: IsolationLevel) (cmgr: ConnectionManager) f =
    let transactionalWithIsolation' (conn: IDbConnection) = 
        let id = Guid.NewGuid().ToString()
        use tx = conn.BeginTransaction(isolation)
        logf "started tx %s" id
        try
            let r = f (withTransaction tx)
            tx.Commit()
            logf "committed tx %s" id
            r
        with e ->
            tx.Rollback()
            logf "rolled back tx %s" id
            reraise()
    fun () -> doWithConnection cmgr transactionalWithIsolation'

/// Wraps a function in a transaction
let transactional a = 
    transactionalWithIsolation IsolationLevel.Unspecified a

/// Transaction result
type TxResult<'a> = Success of 'a | Failure of exn

/// Wraps a function in a transaction, returns a <see cref="TxResult{T}"/>
let transactional2 (cmgr: ConnectionManager) f =
    let transactional2' (conn: IDbConnection) =
        let tx = conn.BeginTransaction()
        try
            let r = f (withConnection conn) 
            tx.Commit()
            Success r
        with e ->
            tx.Rollback()
            Failure e
    fun () -> doWithConnection cmgr transactional2'

/// True if the value is a DB null
let isNull a = DBNull.Value.Equals a

/// Reads a field from a <see cref="IDataRecord"/>, returns None if null, otherwise Some x
let readField (field: string) (record: #IDataRecord) : 'a option =
    try
        let o = record.[field]
        Option.fromDBNull o
    with e ->
        let msg = sprintf "Error reading field %s" field
        raise <| Exception(msg, e)

/// Reads an integer field from a <see cref="IDataRecord"/>, returns None if null, otherwise Some x
let readInt : string -> #IDataRecord -> int option = readField 

/// Reads a string field from a <see cref="IDataRecord"/>, returns None if null, otherwise Some x
let readString : string -> #IDataRecord -> string option = readField 

/// Maps a <see cref="IDataReader"/> as a scalar result
let mapScalar (dr: #IDataReader) =
    try
        if dr.Read()
            then Option.fromDBNull dr.[0]
            else failwith "No results"
    finally
        dr.Dispose()

/// Executes the query, and returns the first column of the first row in the resultset returned by the query. Extra columns or rows are ignored.
let execScalar a b c =
    execReader a b c |> mapScalar

/// Executes the stored procedure, and returns the first column of the first row in the resultset returned by the query. Extra columns or rows are ignored.
let execSPScalar a b c =
    execSPReader a b c |> mapScalar

/// Maps a datareader
let map mapper datareader =
    datareader |> Seq.ofDataReader |> Seq.map mapper

/// Maps a datareader's first row
let mapFirst mapper datareader =
    let r = datareader 
            |> map mapper
            |> Seq.truncate 1
            |> Seq.toList
    if r.Length = 0
        then None
        else Some r.[0]

/// Maps a datareader's single row. Throws if there isn't exactly one row
let mapOne mapper datareader =
    datareader
    |> map mapper
    |> Enumerable.Single

let asNameValue (r: IDataRecord) =
    let names = {0..r.FieldCount-1} |> Seq.map r.GetName
    let values = {0..r.FieldCount-1} |> Seq.map r.GetValue
    Seq.zip names values

let asMap r = r |> asNameValue |> Map.ofSeq
let asDict r = r |> asNameValue |> dict

/// <summary>
/// Converts a mapper into an optional mapper. 
/// Intended to be used when mapping nullable joined tables
/// </summary>
/// <param name="fieldName">Field to use to check for null entity</param>
let optionalBy fieldName mapper r =
    match r |> readField fieldName with
    | None -> None
    | _ -> Some (mapper r)

/// Gets all field names from a record type
let recordFields t = 
    FSharpType.GetRecordFields t |> Array.map (fun p -> p.Name)

/// Gets all field values from a record
let recordValues o = 
    FSharpType.GetRecordFields (o.GetType()) 
    |> Seq.map (fun f -> f.GetValue(o, null))

let internal fieldAlias alias = 
    Array.map (fun s -> sprintf "%s.%s %s_%s" alias s alias s)

let internal sjoin (sep: string) (strings: string[]) = 
    String.Join(sep, strings)

/// Gets all field names from a record type formatted with an alias.
/// E.g. with a field "id" and alias "a", returns "a.id a_id"
let recordFieldsAlias ty alias = 
    recordFields ty |> fieldAlias alias |> sjoin ","
