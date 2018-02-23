open System
open FsSql.Tests
open FsSql.Tests.FsSqlTests
open Expecto

let runTests() =
    let t1 = testList "Sequenced" (nonParallelizableTests::persistentDBTests) |> testSequenced
    let t2 = testList "Parallel" (otherParallelizableTests::memDBTests)
    let t = testList "All" [t1;t2]
    runTests defaultConfig t

let runAdventureWorks() =
    AdventureWorksTests.select()
    AdventureWorksTests.storedProcedure()
    printfn "%s" (String.concat Environment.NewLine AdventureWorksTests.log)
    0

[<EntryPoint>]
let main args = 
    //runAdventureWorks()
    runTests()
