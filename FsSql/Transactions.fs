module Tx

open System
open System.Data
open Sql
open FsSqlPrelude
        
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

type M<'a> = ConnectionManager -> TxResult<'a>

let bind m f cmgr = 
    try
        match m cmgr with
        | Success a -> f a cmgr
        | x -> x
    with e -> Failure e

type TransactionBuilder() =
    member x.Bind(m,f) = bind m f
    member x.Return a = 
        fun (cmgr: ConnectionManager) -> Success a

let execNonQuery sql parameters mgr = 
    Sql.execNonQuery mgr sql parameters |> Success