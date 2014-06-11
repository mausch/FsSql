namespace FsSqlImpl

open System.Data

[<AbstractClass>]
type DbCommandWrapper(cmd: IDbCommand) as this =
    abstract Cancel: unit -> unit
    default x.Cancel() = cmd.Cancel()

    abstract CreateParameter: unit -> IDbDataParameter
    default x.CreateParameter() = cmd.CreateParameter()

    abstract Dispose: unit -> unit
    default x.Dispose() = cmd.Dispose()

    abstract ExecuteNonQuery: unit -> int
    default x.ExecuteNonQuery() = cmd.ExecuteNonQuery()

    abstract ExecuteReader: unit -> IDataReader
    default x.ExecuteReader() = cmd.ExecuteReader()

    abstract ExecuteReader: CommandBehavior -> IDataReader
    default x.ExecuteReader b = cmd.ExecuteReader b

    abstract ExecuteScalar: unit -> obj
    default x.ExecuteScalar() = cmd.ExecuteScalar()

    abstract Prepare: unit -> unit
    default x.Prepare() = cmd.Prepare()

    abstract CommandText: string with get,set
    default x.CommandText
        with get() = cmd.CommandText
        and set v = cmd.CommandText <- v

    abstract CommandTimeout: int with get,set
    default x.CommandTimeout
        with get() = cmd.CommandTimeout
        and set v = cmd.CommandTimeout <- v

    abstract CommandType: CommandType with get,set
    default x.CommandType
        with get() = cmd.CommandType
        and set v = cmd.CommandType <- v

    abstract Connection: IDbConnection with get,set
    default x.Connection
        with get() = cmd.Connection
        and set v = cmd.Connection <- v

    abstract Parameters: IDataParameterCollection with get
    default x.Parameters = cmd.Parameters

    abstract Transaction: IDbTransaction with get,set
    default x.Transaction
        with get() = cmd.Transaction
        and set v = cmd.Transaction <- v

    abstract UpdatedRowSource: UpdateRowSource with get,set
    default x.UpdatedRowSource
        with get() = cmd.UpdatedRowSource
        and set v = cmd.UpdatedRowSource <- v
 
    interface IDbCommand with
        member x.Cancel() = this.Cancel()
        member x.CreateParameter() = this.CreateParameter()
        member x.Dispose() = this.Dispose()
        member x.ExecuteNonQuery() = this.ExecuteNonQuery()
        member x.ExecuteReader() = this.ExecuteReader()
        member x.ExecuteReader(b: CommandBehavior) = this.ExecuteReader b
        member x.ExecuteScalar() = this.ExecuteScalar()
        member x.Prepare() = this.Prepare()
        member x.CommandText
            with get() = this.CommandText
            and set v = this.CommandText <- v
        member x.CommandTimeout
            with get() = this.CommandTimeout
            and set v = this.CommandTimeout <- v
        member x.CommandType
            with get() = this.CommandType
            and set v = this.CommandType <- v
        member x.Connection
            with get() = this.Connection
            and set v = this.Connection <- v
        member x.Parameters 
            with get() = this.Parameters
        member x.Transaction
            with get() = this.Transaction
            and set v = this.Transaction <- v
        member x.UpdatedRowSource
            with get() = this.UpdatedRowSource
            and set v = this.UpdatedRowSource <- v
            