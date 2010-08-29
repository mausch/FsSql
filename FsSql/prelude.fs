module FsSqlPrelude

open System
open Microsoft.FSharp.Reflection

let log s = () // printfn "%A: %s" DateTime.Now s
let logf a = sprintf a >> log

let withResource create dispose action =
    let x = create()
    try
        action x
    finally
        dispose x

let optionToDBNull (a: obj): obj =
    if not (FSharpValue.IsOption a)
        then invalidArg "a" ""
        else
            match a with
            | FSharpValue.OSome x -> x
            | _ -> box DBNull.Value
    