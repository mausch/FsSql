module FsSql.Tests

open Xunit
open System.Data
open System.Linq
open FsSql
open Microsoft.FSharp.Collections

let catch defaultValue f a =
    try
        f a
    with e -> defaultValue

let expand f = fun _ -> f

let createConnection() =
    let conn = new System.Data.SQLite.SQLiteConnection("Data Source=:memory:;Version=3;New=True")
    conn.Open()
    conn

let conn = createConnection()

let execNonQueryF a = execNonQueryF conn a
let execNonQuery a = execNonQuery conn a
let runQuery a = execReaderF conn a

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
    execNonQuery 
        "insert into person (id, name) values (@id, @name)"
        ["@id", upcast p.id; "@name", upcast p.name]
    //execNonQuery "insert into person (id, name) values (%d, %s)" p.id p.name

let updateUser (p: Person) =
    execNonQueryF "update person set name = %s where id = %d" p.name p.id

let countUsers () : int64 = 
    execScalar conn "select count(*) from person" []

let deleteUser = execNonQueryF "delete person where id = %d"

[<Fact>]
let ``insert then get``() = 
    insertUser {id = 1; name = "pepe"; address = None}
    printfn "count: %d" (countUsers())
    let p = getUser 1
    printfn "id=%d, name=%s" p.id p.name
    ()

[<Fact>]
let ``get many``() =
    for i in 1..100 do
        insertUser {id = i; name = "pepe" + i.ToString(); address = None}
    let first10 = runQuery "select * from person" |> Seq.ofDataReader |> Seq.truncate 10
    for i in first10 do
        printfn "%d" (readInt "id" i).Value
    printfn "end!"

[<Fact>]
let ``transaction with exception`` () =
    let someTran () =
        insertUser {id = 1; name = "pepe"; address = None}
        insertUser {id = 2; name = "jose"; address = None}
        failwith "Bla"
        ()

    let someTran = transactional conn (expand someTran)
    let someTran = catch () someTran
    someTran()
    Assert.Equal(0L, countUsers())
    ()

[<Fact>]
let ``transaction with option`` () =
    let someTran conn () =
        insertUser {id = 1; name = "pepe"; address = None}
        insertUser {id = 2; name = "jose"; address = None}
        failwith "Bla"
        5

    let someTran = transactional2 conn someTran
    match someTran() with
    | Success v -> printfn "Success %d" v
    | Failure e -> printfn "Failed with exception %A" e
    Assert.Equal(0L, countUsers())
    ()

// Tests whether n is prime - expects n > 1
// From http://tomasp.net/blog/fsparallelops.aspx
let isPrime n =
    // calculate how large divisors should we test..
    let max = int (sqrt (float n))
    // try to divide n by 2..max (stops when divisor is found)
    not ({ 2..max } |> Seq.filter (fun d -> n%d = 0) |> Enumerable.Any)

[<Fact>]
let ``pseq isprime`` () =
    let p = {100000..800000}
            |> PSeq.filter isPrime
            |> PSeq.length

    printfn "%d primes" p
    ()

let insertUsers () =
    log "inserting"
    let insert () =
        for i in 100000000..100050000 do
            insertUser {id = i; name = "pepe" + i.ToString(); address = None}
    let insert = transactionalWithIsolation IsolationLevel.ReadCommitted conn (fun _ -> insert)
    insert()
    
[<Fact>]
let ``datareader is parallelizable`` () =
    insertUsers()
    log "reading"
    let primes = execReader conn "select * from person" []
                 |> Seq.ofDataReader
                 |> Seq.map (fun r -> (r |> readInt "id").Value)
                 |> PSeq.filter isPrime
                 |> PSeq.length
    logf "%d primes" primes

[<Fact>]
let ``datareader to seq is cacheable`` () =
    insertUsers()
    // this doesn't dispose the data reader
    let all = execReader conn "select * from person" []
               |> Seq.ofDataReader
               |> Seq.cache
    all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r |> readInt "id").Value)
    all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r |> readString "name").Value)
    ()

[<Fact>]
let ``datareader to seq is cacheable 2`` () =
    insertUsers()
    // this doesn't dispose the data reader either!
    use all = execReader conn "select * from person" []
    let all = all
              |> Seq.ofDataReader
              |> Seq.cache
    all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r |> readInt "id").Value)
    all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r |> readString "name").Value)
    ()