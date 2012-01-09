open System
open FsSql.Tests
open Fuchu

[<EntryPoint>]
let main args =
    let r1 = TestList (nonParallelizableTests @ persistentDBTests) |> flattenEvalSeq |> sumTestResults
    let r2 = TestList (otherParallelizableTests @ memDBTests) |> flattenEvalPar |> sumTestResults
    let r = r1 + r2
    Console.WriteLine r
    testResultCountsToErrorLevel r
