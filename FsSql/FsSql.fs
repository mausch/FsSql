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

type ConnectionManager = (unit -> IDbConnection) * (IDbConnection -> unit)

let withConnection (conn: IDbConnection) : ConnectionManager =
    let id = Guid.NewGuid().ToString()
    let create() = 
        logf "creating connection from const %s" id
        conn
    let dispose c = logf "disposing connection (but not really) %s" id
    create,dispose

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
        unbox (worker (query.Value, []))
    else
        let rec getFlattenedFunctionElements (functionType: Type) =
            let domain, range = FSharpType.GetFunctionElements functionType
            if not (FSharpType.IsFunction range)
                then domain::[range]
                else domain::getFlattenedFunctionElements(range)
        let types = getFlattenedFunctionElements typeof<'a>
        let rec proc (types: Type list) (values: obj list) (a: obj) : obj =
            let values = a::values
            match types with
            | [x;_] -> 
                let result = worker (query.Value, List.rev values)
                box result
            | x::y::z::xs -> 
                let cont = proc (y::z::xs) values
                let ft = FSharpType.MakeFunctionType(y,z)
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

let execReaderF connectionFactory a = PrintfFormatProc (sqlProcessorToDataReader connectionFactory) a
let execNonQueryF connectionFactory a = PrintfFormatProc (sqlProcessorNonQuery connectionFactory) a

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

let addParameter (cmd: #IDbCommand) (p: Parameter) =
    let par = cmd.CreateParameter()
    par.DbType <- p.DbType
    par.Direction <- p.Direction
    par.ParameterName <- p.ParameterName
    par.Value <- p.Value
    cmd.Parameters.Add par |> ignore
    cmd

let internal prepareCommand (connection: #IDbConnection) (sql: string) (cmdType: CommandType) (parameters: Parameter list) =
    let cmd = connection.CreateCommand()
    cmd.CommandText <- sql
    cmd.CommandType <- cmdType
    parameters |> Seq.iter (addParameter cmd >> ignore)
    cmd

let internal inferParameterDbType (p: string * obj) = 
    Parameter.make(fst p, snd p)

let parameters (p: (string * obj) list) = 
    p |> List.map inferParameterDbType

let execReader (cmgr: ConnectionManager) (sql: string) (parameters: Parameter list) =
    let create,dispose = cmgr
    let connection = create()
    use cmd = prepareCommand connection sql CommandType.Text parameters
    let dispose() = dispose connection
    new DataReaderWrapper(cmd.ExecuteReader(), dispose) :> IDataReader    

let execNonQuery (cmgr: ConnectionManager) (sql: string) (parameters: Parameter list) =
    let execNonQuery' connection = 
        use cmd = prepareCommand connection sql CommandType.Text parameters
        cmd.ExecuteNonQuery()
    doWithConnection cmgr execNonQuery'

let transactionalWithIsolation (isolation: IsolationLevel) (cmgr: ConnectionManager) (f: ConnectionManager -> 'a -> 'b) (a: 'a) =
    let transactionalWithIsolation' (conn: IDbConnection) = 
        let id = Guid.NewGuid().ToString()
        use tx = conn.BeginTransaction(isolation)
        logf "started tx %s" id
        try
            let r = f (withConnection conn) a
            tx.Commit()
            logf "committed tx %s" id
            r
        with e ->
            tx.Rollback()
            logf "rolled back tx %s" id
            reraise()
    doWithConnection cmgr transactionalWithIsolation'

let transactional a = 
    transactionalWithIsolation IsolationLevel.Unspecified a

type TxResult<'a> = Success of 'a | Failure of exn

let transactional2 (cmgr: ConnectionManager) (f: ConnectionManager -> 'a -> 'b) (a: 'a) =
    let transactional2' (conn: IDbConnection) =
        let tx = conn.BeginTransaction()
        try
            let r = f (withConnection conn) a
            tx.Commit()
            Success r
        with e ->
            tx.Rollback()
            Failure e
    doWithConnection cmgr transactional2'

let isNull a = DBNull.Value.Equals a

let readField (field: string) (record: #IDataRecord) : 'a option =
    let o = record.[field]
    if isNull o
        then None
        else Some (unbox o)

let readInt : string -> #IDataRecord -> int option = readField 

let readString : string -> #IDataRecord -> string option = readField 

let mapScalar (dr: #IDataReader) =
    try
        if dr.Read()
            then unbox dr.[0]
            else failwith "No results"
    finally
        dr.Dispose()

let execScalar a b c =
    execReader a b c |> mapScalar

let writeOption = 
    function
    | None -> box DBNull.Value
    | Some x -> box x

let findOne mapper query id =
    let r = query id
            |> Seq.ofDataReader
            |> Seq.map mapper
            |> Seq.truncate 1
            |> Seq.toList
    if r.Length = 0
        then None
        else Some r.[0]

let getOne mapper query id =
    query id
    |> Seq.ofDataReader
    |> Seq.map mapper
    |> Enumerable.Single
