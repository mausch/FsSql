module FsSql.Tests

open Xunit
open System.Data
open System.Linq
open FsSql

let conn = new System.Data.SQLite.SQLiteConnection("Data Source=:memory:;Version=3;New=True")
conn.Open()
let createConnection () : IDbConnection = upcast conn
let execNonQuery a = runQuery createConnection a
let runQuery a = runQueryToReader createConnection a

type Address = {
    id: int
    street: string
    city: string
}

type Person = {
    id: int
    name: string
    address: Address option
}

let createSchema() =
    use cmd = conn.CreateCommand()
    cmd.CommandText <- "create table person (id int primary key not null, name varchar not null, address int null)"
    cmd.ExecuteNonQuery() |> ignore
    cmd.CommandText <- "create table address (id int primary key not null, street varchar null, city varchar null)"
    cmd.ExecuteNonQuery() |> ignore
    ()

let userMapper r = 
    { id = (readInt "id" r).Value ; name = (readString "name" r).Value; address = None}

let getUser = 
    runQuery "select * from person where id = %d" |> getOne userMapper

let findUser =
    runQuery "select * from person where id = %d" |> findOne userMapper

let insertUser (p: Person) =
    execNonQuery "insert into person (id, name) values (%d, %s)" p.id p.name

let updateUser (p: Person) =
    execNonQuery "update person set name = %s where id = %d" p.name p.id

[<Fact>]
let ``insert then get``() = 
    createSchema()
    insertUser {id = 1; name = "pepe"; address = None}
    let p = getUser 1
    printfn "id=%d, name=%s" p.id p.name
    ()