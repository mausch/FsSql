open System
open FsSql.Tests
open FsSql.Tests.FsSqlTests
open Fuchu
open Fuchu.Impl

let runTests() = 
    let r1 = TestList (nonParallelizableTests @ persistentDBTests) |> evalSeq |> sumTestResults
    let r2 = TestList (otherParallelizableTests @ memDBTests) |> evalPar |> sumTestResults
    let r = r1 + r2
    Console.WriteLine r
    TestResultCounts.errorCode r

let runAdventureWorks() =
    AdventureWorksTests.select()
    AdventureWorksTests.storedProcedure()
    printfn "%s" (String.concat Environment.NewLine AdventureWorksTests.log)
    0

[<EntryPoint>]
let main args = 
    //runAdventureWorks()
    runTests()
