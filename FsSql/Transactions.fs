module Tx

open System
open System.Data
open Sql
open FsSqlPrelude
open Microsoft.FSharp.Reflection

/// Transaction result
type TxResult<'a,'b> =
    /// Transaction committed successfully
    | Commit of 'a
    /// Transaction manually rolled back
    | Rollback of 'b
    /// Transaction failed due to an exception and was rolled back
    | Failed of exn

/// Wraps a function in a transaction with the specified <see cref="IsolationLevel"/>
let transactionalWithIsolation (isolation: IsolationLevel) (f: ConnectionManager -> 'a) (cmgr: ConnectionManager) : TxResult<'a, 'b> =
    let transactionalWithIsolation' (conn: IDbConnection) =
        let id = Guid.NewGuid().ToString()
        logf "starting tx %s" id
        use tx = conn.BeginTransaction(isolation)
        logf "started tx %s" id
        try
            let r = f (withTransaction tx)
            tx.Commit()
            logf "committed tx %s" id
            Commit r
        with e ->
            logf "got exception in tx %s : \n%A" id e
            if isNull tx.Connection then
                logf "WARNING: null connection in tx %s" id
            logf "rolling back tx %s" id
            tx.Rollback()
            logf "rolled back tx %s" id
            Failed e
    doWithConnection cmgr transactionalWithIsolation'

/// Wraps a function in a transaction
let transactional a =
    transactionalWithIsolation IsolationLevel.Unspecified a

//type M<'a,'b> = ConnectionManager -> TxResult<'a,'b>

let bind f m =
    fun (cmgr: ConnectionManager) ->
        try
            match m cmgr with
            | Commit a -> f a cmgr
            | Rollback a -> Rollback a
            | Failed e -> Failed e
        with e ->
            Failed e

let inline mreturn a =
    fun (cmgr: ConnectionManager) -> Commit a

let inline combine m2 m1 =
    m1 |> bind (fun _ -> m2)

type TransactionBuilder(?isolation: IsolationLevel) =
    member x.Delay f = f()

    member x.Bind(m, f) = bind f m

    member x.Combine(m1, m2) = combine m2 m1

    member x.Return a = mreturn a

    member x.Zero() = x.Return ()

    member x.For(sequence, f) =
        fun (cmgr: ConnectionManager) ->
            let folder result element =
                try
                    match result with
                    | Commit() -> f element cmgr
                    | Rollback a -> Rollback a
                    | Failed ex -> Failed ex
                with ex -> Failed ex

            sequence
            |> Seq.fold folder (Commit())

    member x.TryFinally(m: ConnectionManager -> TxResult<_,_>, f: unit -> unit) =
        fun (cmgr: ConnectionManager) ->
            try
                m cmgr
            finally
                f()

    member x.Using(a: #IDisposable, f) =
        let dispose() = if a <> null then a.Dispose()
        x.TryFinally(f a, dispose)

(*
    member x.While(cond: unit -> bool, m: ConnectionManager -> TxResult<_,_>) =
        //let s = Seq.initInfinite (fun _ -> cond()) |> Seq.takeWhile id
        let s = Seq.unfold (fun s -> if cond() then Some((),s) else None) (Some())
        let f i c = m c
        x.For(s, f)
*)

    member x.TryWith(m, handler) =
        fun (cmgr: ConnectionManager) ->
            match m cmgr with
            | Commit a -> Commit a
            | Rollback a -> Rollback a
            | Failed e -> handler e cmgr

    member x.Run (f: ConnectionManager -> TxResult<'a,_>) =
        let subscribe (tx: IDbTransaction) (onCommit: IDbTransaction -> 'a -> unit) (onRollback: IDbTransaction -> _ -> unit) (onFailed: IDbTransaction -> _ -> unit) =
            let r = f (withTransaction tx)
            match r with
            | Commit a -> 
                onCommit tx a
                Commit a
            | Rollback a ->
                onRollback tx a
                Rollback a
            | Failed e ->
                onFailed tx e
                Failed e

        let transactional (conn: IDbConnection) =
            let il = defaultArg isolation IsolationLevel.Unspecified
            let tx = conn.BeginTransaction(il)
            let onCommit (tran: IDbTransaction) _ = tran.Commit()
            let onRollback (tran: IDbTransaction) _ = tran.Rollback()
            let onFailed (tran: IDbTransaction) _ = tran.Rollback()
            subscribe tx onCommit onRollback onFailed

        fun cmgr ->
            match cmgr.tx with
            | None -> doWithConnection cmgr transactional
            | Some t -> subscribe t (fun _ _ -> ()) (fun _ _ -> ()) (fun _ _ -> ())

/// Executes a SQL statement and returns the number of rows affected.
/// For use within a tx monad.
let execNonQuery sql parameters mgr =
    Sql.execNonQuery mgr sql parameters |> Commit

/// Executes a SQL statement.
/// For use within a tx monad.
let execNonQueryi sql parameters mgr =
    Sql.execNonQuery mgr sql parameters |> ignore |> Commit

// TODO, problematic
//let execNonQueryF sql parameters mgr =
//    Sql.execNonQueryF mgr sql parameters |> Success

/// Executes a query and returns a data reader.
/// For use within a tx monad.
let execReader sql parameters mgr =
    Sql.execReader mgr sql parameters |> Commit

/// Executes a query and returns a scalar.
/// For use within a tx monad.
let execScalar sql parameters mgr =
    Sql.execScalar mgr sql parameters |> Commit

/// Rolls back the transaction.
/// For use within a tx monad.
let rollback a (mgr: ConnectionManager) = Rollback a

let map f m =
    fun (conn: ConnectionManager) ->
        match m conn with
        | Commit a -> f a |> Commit
        | Failed x -> Failed x
        | Rollback x -> Rollback x

let get =
    function
    | Commit x -> x
    | Failed e -> raise (Exception("Transaction failed", e))
    | Rollback _ -> failwith "rollback"

let toOption =
    function
    | Commit x -> Some x
    | _ -> None

module Operators =
    let inline (>>=) f x = bind x f
    let inline (>>.) f x = bind (fun _ -> x) f
