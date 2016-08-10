module FsSql.Tests.NorthwindTests

open System
open System.Collections.Generic
open System.Data
open Microsoft.FSharp.Reflection

open FsSql

#if __MonoSQL__
open Mono.Data.Sqlite
type SQLiteConnection = SqliteConnection
type SQLiteException = SqliteException
#else
open System.Data.SQLite
#endif


let createConnection() =
    let conn = new SQLiteConnection("Data Source=test.db;Version=3;New=True;")
    conn.Open()
    conn :> IDbConnection

let createSchema types =
    let exec a = Sql.execNonQuery (Sql.withNewConnection createConnection) a [] |> ignore
    let sqlType t =
        match t with
        | x when x = typeof<int> -> "int"
        | x when x = typeof<string> -> "varchar"
        | x when x = typeof<bool> -> "int"
        | x when x = typeof<DateTime> -> "datetime"
        | x -> failwithf "Don't know how to express type %A in database" x
    let escape s =
        let keywords = HashSet<_>(["order"], StringComparer.InvariantCultureIgnoreCase)
        if keywords.Contains s
            then sprintf "\"%s\"" s // sqlite-specific quote
            else s
    let createTable (escape: string -> string) (sqlType: Type -> string) (t: Type) =
        let fields = FSharpType.GetRecordFields t |> Seq.filter (fun p -> p.Name <> "id")
        let table = escape t.Name
        let drop = sprintf "drop table if exists %s" table
        let fields = 
            let fieldType (t: Type) =
                let nullable = t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>>
                let sqlType = 
                    let t = 
                        if nullable
                            then t.GetGenericArguments().[0]
                            else t
                    sqlType t
                let nullable = if nullable then "" else "not"
                sprintf "%s %s null" sqlType nullable
            fields |> Seq.map (fun f -> sprintf "%s %s" (escape f.Name) (fieldType f.PropertyType))
        let fields = String.Join(",", Seq.toArray fields)
        let create = sprintf "create table %s (id int primary key, %s)" table fields
        printfn "%s" create
        exec drop
        exec create
        ()
    types |> Seq.iter (createTable escape sqlType)

type Employee = {
    id: int
    LastName: string
    FirstName: string
    ReportsTo: int option
}

type Order = {
    id: int
    customer: int
    employee: int
    date: DateTime
}

type Product = {
    id: int
    ProductName: string
}

type OrderDetail = {
    id: int
    order: int
    product: int
}


let ``something``() =
    createSchema [typeof<Employee>; typeof<Order>; typeof<Product>; typeof<OrderDetail>]
    ()