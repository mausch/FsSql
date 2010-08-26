module FsSqlPrelude

open System

let log s = () // printfn "%A: %s" DateTime.Now s
let logf a = sprintf a >> log

let withResource create dispose action =
    let x = create()
    try
        action x
    finally
        dispose x

let isOption (a: obj) =
    if a = null
        then false
        else
            let t = a.GetType()
            if not t.IsGenericType 
                then false
                else t.GetGenericTypeDefinition() = typedefof<option<_>>

let (|OptionType|NotOptionType|) x = 
    if isOption x 
        then OptionType
        else NotOptionType

let getOptionValue (a: obj): 'a =
    if not (isOption a)
        then invalidArg "a" ""
        else unbox <| a.GetType().GetProperty("Value").GetValue(a, null)

let isNone (a: obj): bool =
    if not (isOption a)
        then invalidArg "a" ""
        else unbox <| a.GetType().GetMethod("get_IsNone").Invoke(null, [| a |])

let isSome (a: obj): bool =
    if not (isOption a)
        then invalidArg "a" ""
        else unbox <| a.GetType().GetMethod("get_IsSone").Invoke(null, [| a |])

let (|OSome|_|) (x: obj) =
    if not (isOption x)
        then invalidArg "a" ""
        else 
            if isNone x
                then None
                else Some (getOptionValue x)

let optionToDBNull (a: obj): obj =
    if not (isOption a)
        then invalidArg "a" ""
        else
            match a with
            | OSome x -> x
            | _ -> box DBNull.Value
    