namespace FsSql.Tests

[<AutoOpen>]
module TestPrelude =

    open Fuchu
    open System

    let failwithe (e: #exn) msg = raise <| System.Exception(msg, e)
    let failwithef (e: #exn) = Printf.kprintf (failwithe e)
