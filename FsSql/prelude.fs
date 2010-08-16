module FsSqlPrelude

open System

let log s = printfn "%A: %s" DateTime.Now s
let logf a = sprintf a >> log
