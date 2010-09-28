module Tx

open System
open System.Data
open Sql
open FsSqlPrelude

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

