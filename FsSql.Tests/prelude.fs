namespace FsSql.Tests

[<AutoOpen>]
module TestPrelude =

    open Fuchu
    open System

    let catch defaultValue f a =
        try
            f a
        with e -> defaultValue

    let failwithe (e: #exn) msg = raise <| System.Exception(msg, e)
    let failwithef (e: #exn) = Printf.kprintf (failwithe e)
