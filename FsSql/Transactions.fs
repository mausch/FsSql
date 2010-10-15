module Tx

open System
open System.Data
open Sql
open FsSqlPrelude
open Microsoft.FSharp.Reflection

        
/// Wraps a function in a transaction with the specified <see cref="IsolationLevel"/>
let transactionalWithIsolation (isolation: IsolationLevel) f cmgr =
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
    doWithConnection cmgr transactionalWithIsolation'

/// Wraps a function in a transaction
let transactional a = 
    transactionalWithIsolation IsolationLevel.Unspecified a

/// If there is a running transaction, the function executes within this transaction.
/// Otherwise, throws.
let mandatory f cmgr =
    let _,_,tx = cmgr
    match tx with
    | Some _ -> f cmgr
    | None -> failwith "Transaction required!"

/// If there is a running transaction, throws.
/// Otherwise, the function executes without any transaction.
let never f cmgr =
    let _,_,tx = cmgr
    match tx with
    | Some _ -> failwith "Transaction present!"
    | None -> f cmgr

/// If there is a running transaction, the function executes within this transaction.
/// Otherwise, the function executes without any transaction.
let supported f cmgr = f cmgr

/// If there is a running transaction, the function executes within this transaction.
/// Otherwise, a new transaction is started and the function executes within this new transaction.
let required f cmgr = 
    let _,_,tx = cmgr
    let g = 
        match tx with
        | None -> transactional
        | _ -> id
    (g f) cmgr

/// Transaction result
type TxResult<'a,'b> = 
    /// Transaction committed successfully
    | Commit of 'a 
    /// Transaction manually rolled back
    | Rollback of 'b
    /// Transaction failed due to an exception and was rolled back
    | Failed of exn

/// <summary>
/// Wraps a function in a transaction, returns a <see cref="TxResult"/>
/// </summary>
let transactional2 f (cmgr: ConnectionManager) =
    let transactional2' (conn: IDbConnection) =
        let tx = conn.BeginTransaction()
        try
            let r = f (withTransaction tx)
            tx.Commit()
            Commit r
        with e ->
            tx.Rollback()
            Failed e
    doWithConnection cmgr transactional2'

//type M<'a,'b> = ConnectionManager -> TxResult<'a,'b>

type TransactionBuilder() =
    member x.Delay f = f()

    member x.Bind(m, f) =
        fun (cmgr: ConnectionManager) ->
            try
                match m cmgr with
                | Commit a -> f a cmgr
                | Rollback a -> Rollback a
                | Failed e -> Failed e
            with e -> 
                Failed e

    member x.Combine(m1, m2) = 
        x.Bind(m1, fun() -> m2)

    member x.Return a = 
        fun (cmgr: ConnectionManager) -> Commit a

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

    member x.Using(a, f) = 
        let dispose() = (a :> IDisposable).Dispose()
        x.TryFinally(f a, dispose)

    member x.Run (f: ConnectionManager -> TxResult<'a,_>) =         
        let subscribe (tx: IDbTransaction) (onCommit: IDbTransaction -> 'a -> TxResult<'a,_>) = 
            let r = f (withTransaction tx)
            match r with
            | Commit a -> onCommit tx a
            | Rollback a ->
                tx.Rollback()
                Rollback a
            | Failed e ->
                tx.Rollback()
                Failed e

        let transactional (conn: IDbConnection) =
            let tx = conn.BeginTransaction()
            let onCommit (tran: IDbTransaction) result = 
                tran.Commit()
                Commit result
            subscribe tx onCommit

        fun cmgr -> 
            let _,_,tx = cmgr
            match tx with
            | None -> doWithConnection cmgr transactional
            | Some t -> subscribe t (fun _ -> Commit)

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

/// Rolls back the transaction.
/// For use within a tx monad.
let rollback a (mgr: ConnectionManager) = Rollback a
