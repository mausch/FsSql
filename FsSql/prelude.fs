module FsSql.Prelude

open System
open Microsoft.FSharp.Reflection
open FsSql.Logging

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
