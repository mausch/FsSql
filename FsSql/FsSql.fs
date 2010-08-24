module Sql

open System
open System.Collections.Generic
open System.Data
open System.Data.SqlClient
open System.Linq
open System.Text.RegularExpressions
open Microsoft.FSharp.Reflection
open FsSqlImpl
open FsSqlPrelude

/// Encapsulates how to create and dispose a database connection
type ConnectionManager = (unit -> IDbConnection) * (IDbConnection -> unit)

/// Creates a <see cref="ConnectionManager"/> with an externally-owned connection
let withConnection (conn: IDbConnection) : ConnectionManager =
    let id = Guid.NewGuid().ToString()
    let create() = 
        logf "creating connection from const %s" id
        conn
    let dispose c = logf "disposing connection (but not really) %s" id
    create,dispose

/// Creates a <see cref="ConnectionManager"/> with an owned connection
let withNewConnection (create: unit -> IDbConnection) : ConnectionManager = 
    let id = Guid.NewGuid().ToString()
    let create() =
        logf "creating connection %s" id
        create()
    let dispose (c: IDisposable) = 
        c.Dispose()
        logf "disposing connection %s" id
    create,dispose

let internal withCreateConnection (create: unit -> IDbConnection) : ConnectionManager = 
    let dispose x = ()
    create,dispose

let internal doWithConnection (cmgr: ConnectionManager) f =
    let create,dispose = cmgr
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
    let create,dispose = cmgr
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
    static member make(parameterName, value) =
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
    par.Value <- p.Value
    cmd.Parameters.Add par |> ignore
    cmd

let internal prepareCommand (connection: #IDbConnection) (sql: string) (cmdType: CommandType) (parameters: #seq<Parameter>) =
    let cmd = connection.CreateCommand()
    cmd.CommandText <- sql
    cmd.CommandType <- cmdType
    parameters |> Seq.iter (addParameter cmd >> ignore)
    cmd

let internal inferParameterDbType (p: string * obj) = 
    Parameter.make(fst p, snd p)

/// Creates a list of parameters
let parameters (p: #seq<string * obj>) = 
    p |> Seq.map inferParameterDbType

/// Executes a query and returns a data reader
let execReader (cmgr: ConnectionManager) (sql: string) (parameters: #seq<Parameter>) =
    let create,dispose = cmgr
    let connection = create()
    use cmd = prepareCommand connection sql CommandType.Text parameters
    let dispose() = dispose connection
    new DataReaderWrapper(cmd.ExecuteReader(), dispose) :> IDataReader    

/// Executes a SQL statement and returns the number of rows affected
let execNonQuery (cmgr: ConnectionManager) (sql: string) (parameters: #seq<Parameter>) =
    let execNonQuery' connection = 
        use cmd = prepareCommand connection sql CommandType.Text parameters
        cmd.ExecuteNonQuery()
    doWithConnection cmgr execNonQuery'

/// Wraps a function in a transaction with the specified <see cref="IsolationLevel"/>
let transactionalWithIsolation (isolation: IsolationLevel) (cmgr: ConnectionManager) f =
    let transactionalWithIsolation' (conn: IDbConnection) = 
        let id = Guid.NewGuid().ToString()
        use tx = conn.BeginTransaction(isolation)
        logf "started tx %s" id
        try
            let r = f (withConnection conn)
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
    let o = record.[field]
    Option.fromDBNull o

/// Reads an integer field from a <see cref="IDataRecord"/>, returns None if null, otherwise Some x
let readInt : string -> #IDataRecord -> int option = readField 

/// Reads a string field from a <see cref="IDataRecord"/>, returns None if null, otherwise Some x
let readString : string -> #IDataRecord -> string option = readField 

/// Maps a <see cref="IDataReader"/> as a scalar result
let mapScalar (dr: #IDataReader) =
    try
        if dr.Read()
            then unbox dr.[0]
            else failwith "No results"
    finally
        dr.Dispose()

/// Executes the query, and returns the first column of the first row in the resultset returned by the query. Extra columns or rows are ignored.
let execScalar a b c =
    execReader a b c |> mapScalar

/// Maps a datareader's first row
let mapFirst mapper datareader =
    let r = datareader
            |> Seq.ofDataReader
            |> Seq.map mapper
            |> Seq.truncate 1
            |> Seq.toList
    if r.Length = 0
        then None
        else Some r.[0]

/// Maps a datareader's single row. Throws if there isn't exactly one row
let mapOne mapper datareader =
    datareader
    |> Seq.ofDataReader
    |> Seq.map mapper
    |> Enumerable.Single

/// Maps a datareader
let map mapper datareader =
    datareader |> Seq.ofDataReader |> Seq.map mapper

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

let internal fieldAlias alias = 
    Array.map (fun s -> sprintf "%s.%s %s_%s" alias s alias s)

let internal sjoin (sep: string) (strings: string[]) = 
    String.Join(sep, strings)

/// Gets all field names from a record type formatted with an alias.
/// E.g. with a field "id" and alias "a", returns "a.id a_id"
let recordFieldsAlias ty alias = 
    recordFields ty |> fieldAlias alias |> sjoin ","
