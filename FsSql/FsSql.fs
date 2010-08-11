module FsSql

open System
open System.Data
open System.Data.SqlClient
open System.Text.RegularExpressions
open Microsoft.FSharp.Reflection

type DbCommandWrapper(cmd: IDbCommand) =
    let doAndDispose f =
        try
            f()
        finally
            cmd.Connection.Dispose()
            cmd.Dispose()
            
    interface IDbCommand with
        member x.Cancel() = cmd.Cancel()
        member x.CreateParameter() = cmd.CreateParameter()
        member x.Dispose() = cmd.Dispose()
        member x.Prepare() = cmd.Prepare()
        member x.CommandText with get() = cmd.CommandText and set v = cmd.CommandText <- v
        member x.CommandTimeout with get() = cmd.CommandTimeout and set v = cmd.CommandTimeout <- v
        member x.CommandType with get() = cmd.CommandType and set v = cmd.CommandType <- v
        member x.Connection with get() = cmd.Connection and set v = cmd.Connection <- v
        member x.Parameters with get() = cmd.Parameters
        member x.Transaction with get() = cmd.Transaction and set v = raise <| NotSupportedException()
        member x.UpdatedRowSource with get() = cmd.UpdatedRowSource and set v = cmd.UpdatedRowSource <- v
        member x.ExecuteNonQuery() = doAndDispose cmd.ExecuteNonQuery
        member x.ExecuteReader() = cmd.ExecuteReader()
        member x.ExecuteReader b = cmd.ExecuteReader b
        member x.ExecuteScalar() = doAndDispose cmd.ExecuteScalar
            


let PrintfFormatProc (worker: string * obj list -> 'd)  (query: PrintfFormat<'a, _, _, 'd>) : 'a =
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

let sqlProcessor (connectionFactory: unit -> #IDbConnection) (sql: string, values: obj list) =
    let stripFormatting s =
        let i = ref -1
        let eval (rxMatch: Match) =
            incr i
            sprintf "@p%d" !i
        Regex.Replace(s, "%.", eval)
    let sql = stripFormatting sql
    let conn = connectionFactory()
    let cmd = conn.CreateCommand()
    cmd.CommandText <- sql
    let createParam i (p: obj) =
        let param = cmd.CreateParameter()
        //param.DbType <- DbType.
        param.ParameterName <- sprintf "@p%d" i
        param.Value <- p
        cmd.Parameters.Add param |> ignore
    values |> Seq.iteri createParam
    cmd

let sqlProcessorToDataReader a b = 
    let cmd = sqlProcessor a b
    cmd.ExecuteReader()

let sqlProcessorToUnit a b =
    let cmd = sqlProcessor a b
    cmd.ExecuteNonQuery() |> ignore

let runQueryToReader connectionFactory a = PrintfFormatProc (sqlProcessorToDataReader connectionFactory) a
let kkk a = sqlProcessorToUnit a
let runQuery connectionFactory a = PrintfFormatProc (sqlProcessorToUnit connectionFactory) a

let transactional (conn: #IDbConnection) f =
    let tx = conn.BeginTransaction()
    try
        let r = f conn
        tx.Commit()
        r
    with e ->
        tx.Rollback()
        reraise()

let mapReader (mapper: #IDataRecord -> _) (dr: #IDataReader) = 
    seq {
        try
            while dr.Read() do
                yield mapper dr 
        finally
            dr.Dispose() }

let readField (field: string) (record: #IDataRecord) : 'a option =
    let o = record.[field]
    if o = upcast DBNull.Value
        then None
        else Some (unbox o)

let readInt : string -> #IDataRecord -> int option = readField 

let readString : string -> #IDataRecord -> string option = readField 

let writeOption = 
    function
    | None -> null
    | Some x -> box x

let getOne mapper runSQL id =
    let r = runSQL id
            |> mapReader mapper
            |> Seq.toList
    if r.Length = 0
        then None
        else Some r.[0]