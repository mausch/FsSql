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

    let inline assertEqual msg expected actual =
        if expected <> actual
            then failtestf "%s\nExpected: %A\nActual: %A" msg expected actual

    let inline assertNone msg = 
        function
        | Some x -> failtestf "Expected None, Actual: Some (%A)" x
        | _ -> ()

    let inline assertRaise (ex: Type) f =
        try
            f()
            failtestf "Expected exception '%s' but no exception was raised" ex.FullName
        with e ->
            if e.GetType() <> ex
                then failtestf "Expected exception '%s' but raised:\n%A" ex.FullName e

