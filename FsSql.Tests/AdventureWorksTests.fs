module AdventureWorksTests

open System
open System.Data
open System.Data.SqlClient
open MbUnit.Framework

let createConnection() =
    let c = new SqlConnection("Data Source=.\\sqlexpress;initial catalog=AdventureWorks;Integrated Security=SSPI;")
    c.Open()
    c :> IDbConnection

let connMgr = Sql.withNewConnection createConnection

[<Test>]
let select() =
    let l = Sql.execReaderF connMgr "select * from HumanResources.Department" |> List.ofDataReader
    printfn "%d" l.Length
    ()

[<Test>]
let ``stored procedure``() =
    let l = Sql.execSPReader connMgr "uspGetEmployeeManagers" (Sql.parameters ["@EmployeeID", box 1]) |> List.ofDataReader
    printfn "%d" l.Length
    ()