namespace FsSql

open System.Data

[<AbstractClass>]
type DbConnectionWrapper(conn: IDbConnection) as this =
    abstract BeginTransaction: unit -> IDbTransaction
    default x.BeginTransaction() = conn.BeginTransaction()

    abstract BeginTransaction: IsolationLevel -> IDbTransaction
    default x.BeginTransaction il = conn.BeginTransaction il

    abstract ChangeDatabase: string -> unit
    default x.ChangeDatabase databaseName = conn.ChangeDatabase databaseName

    abstract Close: unit -> unit
    default x.Close() = conn.Close()

    abstract ConnectionString: string with get,set
    default x.ConnectionString
        with get (): string = conn.ConnectionString
        and set (v: string): unit = conn.ConnectionString <- v

    abstract ConnectionTimeout: int with get
    default x.ConnectionTimeout = conn.ConnectionTimeout

    abstract CreateCommand: unit -> IDbCommand
    default x.CreateCommand() = conn.CreateCommand()

    abstract Database: string with get
    default x.Database = conn.Database

    abstract Dispose: unit -> unit
    default x.Dispose() = conn.Dispose()

    abstract Open: unit -> unit
    default x.Open() = conn.Open()

    abstract State: ConnectionState with get
    default x.State = conn.State

    interface IDbConnection with
        member x.BeginTransaction() = this.BeginTransaction()
        member x.BeginTransaction(il: IsolationLevel) = this.BeginTransaction il
        member x.ChangeDatabase(databaseName: string): unit = this.ChangeDatabase databaseName
        member x.Close(): unit = this.Close()
        member x.ConnectionString
            with get (): string = this.ConnectionString
            and set (v: string): unit = this.ConnectionString <- v
        member x.ConnectionTimeout = this.ConnectionTimeout
        member x.CreateCommand(): IDbCommand = this.CreateCommand()
        member x.Database = this.Database
        member x.Dispose() = this.Dispose()
        member x.Open() = this.Open()
        member x.State = this.State

