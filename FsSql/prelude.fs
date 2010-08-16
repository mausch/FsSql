module FsSqlPrelude

open System

let log s = printfn "%A: %s" DateTime.Now s
let logf a = sprintf a >> log

let withResource (create: unit -> 'a) (dispose: 'a -> unit) (action: 'a -> 'b) =
    let x = create()
    try
        action x
    finally
        dispose x