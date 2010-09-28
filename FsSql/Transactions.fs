﻿module Tx

open System
open System.Data
open Sql
open FsSqlPrelude
        
/// Wraps a function in a transaction with the specified <see cref="IsolationLevel"/>
let transactionalWithIsolation (isolation: IsolationLevel) f =
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
    fun cmgr -> doWithConnection cmgr transactionalWithIsolation'

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
        | Some t -> id
    (g f) cmgr

/// Transaction result
type TxResult<'a> = Success of 'a | Failure of exn

/// <summary>
/// Wraps a function in a transaction, returns a <see cref="TxResult"/>
/// </summary>
let transactional2 f (cmgr: ConnectionManager) =
    let transactional2' (conn: IDbConnection) =
        let tx = conn.BeginTransaction()
        try
            let r = f (withConnection conn) 
            tx.Commit()
            Success r
        with e ->
            tx.Rollback()
            Failure e
    doWithConnection cmgr transactional2'

