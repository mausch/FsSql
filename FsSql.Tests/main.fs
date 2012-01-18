open System
open FsSql.Tests
open Fuchu

[<EntryPoint>]
let main args =
    let r1 = TestList (nonParallelizableTests @ persistentDBTests) |> evalSeq |> sumTestResults
    let r2 = TestList (otherParallelizableTests @ memDBTests) |> evalPar |> sumTestResults
    let r = r1 + r2
    Console.WriteLine r
    r.ToErrorLevel()
