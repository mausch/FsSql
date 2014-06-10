open System
open FsSql.Tests
open FsSql.Tests.FsSqlTests
open Fuchu
open Fuchu.Impl

[<EntryPoint>]
let main args =
    let r1 = TestList (nonParallelizableTests @ persistentDBTests) |> evalSeq |> sumTestResults
    let r2 = TestList (otherParallelizableTests @ memDBTests) |> evalPar |> sumTestResults

    let r3 = run TableValuedParametersTests.tvpTests

    let r = r1 + r2
    Console.WriteLine r
    TestResultCounts.errorCode r