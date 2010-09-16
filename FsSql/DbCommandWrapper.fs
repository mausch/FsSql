namespace FsSqlImpl

open System.Data

type DbCommandWrapper(cmd: IDbCommand, dispose: unit -> unit) = 
    interface IDbCommand with
        member x.Cancel() = cmd.Cancel()
        member x.CreateParameter() = cmd.CreateParameter()
        member x.Dispose() = 
            cmd.Dispose()
            dispose()
        member x.ExecuteNonQuery() = cmd.ExecuteNonQuery()
        member x.ExecuteReader() = cmd.ExecuteReader()
        member x.ExecuteReader(b: CommandBehavior) = cmd.ExecuteReader b
        member x.ExecuteScalar() = cmd.ExecuteScalar()
        member x.Prepare() = cmd.Prepare()
        member x.CommandText
            with get() = cmd.CommandText
            and set v = cmd.CommandText <- v
        member x.CommandTimeout
            with get() = cmd.CommandTimeout
            and set v = cmd.CommandTimeout <- v
        member x.CommandType
            with get() = cmd.CommandType
            and set v = cmd.CommandType <- v
        member x.Connection
            with get() = cmd.Connection
            and set v = cmd.Connection <- v
        member x.Parameters 
            with get() = cmd.Parameters
        member x.Transaction
            with get() = cmd.Transaction
            and set v = cmd.Transaction <- v
        member x.UpdatedRowSource
            with get() = cmd.UpdatedRowSource
            and set v = cmd.UpdatedRowSource <- v
            