module FsSql.Tests

open Xunit
open System.Data
open System.Linq
open FsSql

let catch defaultValue f a =
    try
        f a
    with e -> defaultValue

let conn = new System.Data.SQLite.SQLiteConnection("Data Source=:memory:;Version=3;New=True")
conn.Open()
//let createConnection () : IDbConnection = upcast conn
let execNonQuery a = execNonQuery conn a
let runQuery a = execReader conn a

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

createSchema()

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

let countUsers () = 
    runQuery "select count(*) from person" |> mapCount

let deleteUser = execNonQuery "delete person where id = %d"

[<Fact>]
let ``insert then get``() = 
    insertUser {id = 1; name = "pepe"; address = None}
    printfn "count: %d" (countUsers())
    let p = getUser 1
    printfn "id=%d, name=%s" p.id p.name
    ()

[<Fact>]
let ``get many``() =
    for i in {1..100} do
        insertUser {id = i; name = "pepe" + i.ToString(); address = None}
    let first10 = runQuery "select * from person" |> Seq.ofDataReader |> Seq.truncate 10
    for i in first10 do
        printfn "%d" (readInt "id" i).Value
    printfn "end!"

[<Fact>]
let ``transactions`` () =
    let someTran conn () =
        insertUser {id = 1; name = "pepe"; address = None}
        insertUser {id = 2; name = "jose"; address = None}
        failwith "Bla"
        ()

    let someTran = transactional conn someTran
    let someTran = catch () someTran
    someTran()
    Assert.Equal(0L, countUsers())
    ()