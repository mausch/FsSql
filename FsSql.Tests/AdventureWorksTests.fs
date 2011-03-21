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
let sql = SqlWrapper(connMgr)

[<Test>]
let select() =
    let l = sql.ExecReaderF "select * from HumanResources.Department" |> List.ofDataReader
    printfn "%d" l.Length
    ()

[<Test>]
let ``stored procedure``() =
    let l = sql.ExecSPReader "uspGetEmployeeManagers" (Sql.parameters ["@EmployeeID", box 1]) |> List.ofDataReader
    printfn "%d" l.Length
    ()