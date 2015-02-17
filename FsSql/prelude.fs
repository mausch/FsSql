module FsSqlPrelude

open System
open Microsoft.FSharp.Reflection

let log s = () // printfn "%A: %s" DateTime.Now s
let logf a = Printf.kprintf log a

let inline internal isNull a = LanguagePrimitives.PhysicalEquality a null

let withResource create dispose action =
    let x = create()
    try
        action x
    finally
        dispose x

let optionToDBNull =
    function
    | FSharpValue.OSome x -> x
    | _ -> box DBNull.Value
