module FsSql.Tests.AdventureWorksTests

open System
open System.Data
open System.Data.SqlClient
open FsSql

let log = ResizeArray<string>() // naive log of executed commands

// 
let loggingConnection conn =
    { new DbConnectionWrapper(conn) with
        override x.CreateCommand() =
            let cmd = conn.CreateCommand()
            upcast { new DbCommandWrapper(cmd) with
                override x.ExecuteReader() = 
                    let parameters = 
                        Seq.cast<IDbDataParameter> x.Parameters
                        |> Seq.map (fun p -> sprintf "%s = %A" p.ParameterName p.Value)
                        |> String.concat ","
                    let logMsg = sprintf "%s (%s)" x.CommandText parameters
                    log.Add logMsg
                    cmd.ExecuteReader()

                // override other Execute* methods as needed
            }
    }
            
    

let createConnection() =
    let c = new SqlConnection("Data Source=.\\sql2008r2;initial catalog=AdventureWorks2008R2;Integrated Security=SSPI;")
    let c = loggingConnection c
    c.Open()
    c :> IDbConnection

let connMgr = Sql.withNewConnection createConnection
let sql = SqlWrapper connMgr

let select() =
    let l = sql.ExecReaderF "select * from HumanResources.Department" |> List.ofDataReader
    printfn "%d" l.Length

let storedProcedure() =
    let l = sql.ExecSPReader "uspGetEmployeeManagers" (Sql.parameters ["@BusinessEntityID", box 1]) |> List.ofDataReader
    printfn "%d" l.Length