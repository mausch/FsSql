module FsSql.Tests

open MbUnit.Framework
open System
open System.Data
open System.Data.SQLite
open System.Linq
open FsSql
open FsSqlPrelude
open Microsoft.FSharp.Collections

let assertThrows<'e when 'e :> exn> f =
    let action = Gallio.Common.Action f
    Assert.Throws<'e> action |> ignore

let catch defaultValue f a =
    try
        f a
    with e -> defaultValue

let expand f = fun _ -> f

let createConnection() =
    let conn = new System.Data.SQLite.SQLiteConnection("Data Source=:memory:;Version=3;New=True")
    conn.Open()
    conn

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

let createSchema conn =
    let exec a = FsSql.execNonQuery conn a [] |> ignore
    exec "create table person (id int primary key not null, name varchar not null, address int null)"
    exec "create table address (id int primary key not null, street varchar null, city varchar null)"
    ()

let createConnectionAndSchema() =
    let conn = createConnection()
    createSchema conn
    conn

let withDatabase f = withResource createConnectionAndSchema (fun c -> c.Dispose()) f

let userMapper r = 
    { id = (readInt "id" r).Value ; name = (readString "name" r).Value; address = None}

let selectById conn = execReaderF conn "select * from person where id = %d"

let getUser conn =
    selectById conn |> getOne userMapper

let findUser conn =
    selectById conn |> findOne userMapper

let insertUser conn (p: Person) =
    execNonQuery conn
        "insert into person (id, name) values (@id, @name)"
        (inferParameterDbTypes ["@id", upcast p.id; "@name", upcast p.name])
        |> ignore
    //execNonQuery "insert into person (id, name) values (%d, %s)" p.id p.name

let updateUser conn (p: Person) =
    execNonQueryF conn "update person set name = %s where id = %d" p.name p.id

let countUsers conn : int64 = 
    execScalar conn "select count(*) from person" []

let deleteUser conn = execNonQueryF conn "delete person where id = %d" |> ignore

[<Test>]
[<Parallelizable>]
let ``insert then get``() = 
    withDatabase (fun conn ->
        insertUser conn {id = 1; name = "pepe"; address = None}
        printfn "count: %d" (countUsers conn)
        let p = getUser conn 1
        printfn "id=%d, name=%s" p.id p.name)
    ()

[<Test>]
[<Parallelizable>]
let ``find non-existent record``() =
    withDatabase (fun conn ->
        let p = findUser conn 39393
        Assert.IsTrue p.IsNone
        printfn "end test")

[<Test>]
[<Parallelizable>]
let ``find existent record``() =
    withDatabase (fun conn ->
        insertUser conn {id = 1; name = "pepe"; address = None}
        let p = findUser conn 1
        Assert.IsTrue p.IsSome
        printfn "end test")

[<Test>]
[<Parallelizable>]
let ``get many``() =
    withDatabase (fun conn ->
        for i in 1..100 do
            insertUser conn {id = i; name = "pepe" + i.ToString(); address = None}
        let first10 = execReaderF conn "select * from person" |> Seq.ofDataReader |> Seq.truncate 10
        for i in first10 do
            printfn "%d" (readInt "id" i).Value
        printfn "end!")

[<Test>]
[<Parallelizable>]
let ``transaction with exception`` () =
    let someTran conn =
        insertUser conn {id = 1; name = "pepe"; address = None}
        insertUser conn {id = 2; name = "jose"; address = None}
        failwith "Bla"
        ()

    withDatabase (fun conn ->
        let someTran = transactional conn (expand someTran)
        let someTran = catch () someTran
        someTran conn
        Assert.AreEqual(0L, countUsers conn))
    ()

[<Test>]
[<Parallelizable>]
let ``transaction committed`` () =
    let someTran conn =
        insertUser conn {id = 1; name = "pepe"; address = None}
        insertUser conn {id = 2; name = "jose"; address = None}
        ()

    withDatabase (fun conn ->
        let someTran = transactional conn (expand someTran)
        someTran conn
        Assert.AreEqual(2L, countUsers conn))
    ()

[<Test>]
[<Parallelizable>]
let ``nested transactions are NOT supported`` () =
    let someTran conn () =
        let subtran conn () = 
            insertUser conn {id = 3; name = "jorge"; address = None}
            failwith "this fails"
        (transactional conn subtran)()
        insertUser conn {id = 1; name = "pepe"; address = None}
        insertUser conn {id = 2; name = "jose"; address = None}
        ()

    withDatabase (fun conn ->
        let someTran = transactional conn someTran
        assertThrows<SQLiteException> someTran)
    ()

[<Test>]
[<Parallelizable>]
let ``transaction with option`` () =
    let someTran conn () =
        insertUser conn {id = 1; name = "pepe"; address = None}
        insertUser conn {id = 2; name = "jose"; address = None}
        failwith "Bla"
        5

    withDatabase (fun conn ->
        let someTran = transactional2 conn someTran
        let result = someTran()
        match result with
        | Success v -> printfn "Success %d" v; raise <| Exception("transaction should have failed!")
        | Failure e -> printfn "Failed with exception %A" e
        Assert.AreEqual(0L, countUsers conn))
    ()

// Tests whether n is prime - expects n > 1
// From http://tomasp.net/blog/fsparallelops.aspx
let isPrime n =
    // calculate how large divisors should we test..
    let max = int (sqrt (float n))
    // try to divide n by 2..max (stops when divisor is found)
    not ({ 2..max } |> Seq.filter (fun d -> n%d = 0) |> Enumerable.Any)

[<Test>]
[<Parallelizable>]
let ``pseq isprime`` () =
    let p = {100000..800000}
            |> PSeq.filter isPrime
            |> PSeq.length

    printfn "%d primes" p
    ()

let insertUsers conn =
    log "inserting"
    let insert conn () =
        for i in 100000000..100050000 do
            insertUser conn {id = i; name = "pepe" + i.ToString(); address = None}
    let insert = transactionalWithIsolation IsolationLevel.ReadCommitted conn insert
    insert()
    
[<Test>]
[<Parallelizable>]
let ``datareader is parallelizable`` () =
    withDatabase (fun conn ->
        insertUsers conn
        log "reading"
        let primes = execReader conn "select * from person" []
                     |> Seq.ofDataReader
                     |> Seq.map (fun r -> (r |> readInt "id").Value)
                     |> PSeq.filter isPrime
                     |> PSeq.length
        logf "%d primes" primes)

[<Test>]
let ``datareader to seq is forward-only``() =
    withDatabase (fun conn ->
        insertUsers conn
        let all = execReader conn "select * from person" []
                  |> Seq.ofDataReader
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r |> readInt "id").Value)
        let secondIter() = 
            all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r |> readString "name").Value)
        assertThrows<InvalidOperationException> secondIter)
    ()
    

[<Test>]
[<Parallelizable>]
let ``datareader to seq is cacheable`` () =
    withDatabase (fun conn ->
        insertUsers conn
        // this doesn't dispose the data reader
        let all = execReader conn "select * from person" []
                   |> Seq.ofDataReader
                   |> Seq.cache
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r |> readInt "id").Value)
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r |> readString "name").Value)
    )
    ()

[<Test>]
[<Parallelizable>]
let ``datareader to seq is cacheable 2`` () =
    withDatabase (fun conn ->
        insertUsers conn
        // this doesn't dispose the data reader either!
        use reader = execReader conn "select * from person" []
        let all = reader
                  |> Seq.ofDataReader
                  |> Seq.cache
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r |> readInt "id").Value)
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r |> readString "name").Value)
    )
    ()

[<Test>]
[<Parallelizable>]
let ``datareader to seq is cacheable 3`` () =
    withDatabase (fun conn ->
        insertUsers conn
        // this doesn't dispose the data reader either!
        let reader = execReader conn "select * from person" []
        let withReader reader =
            let all = reader
                      |> Seq.ofDataReader
                      |> Seq.cache
            all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r |> readInt "id").Value)
            all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r |> readString "name").Value)
        using reader withReader
    )
    ()

[<Test>]
[<Parallelizable>]
let ``datareader with lazylist`` () =
    withDatabase (fun conn ->
        insertUsers conn
        // this doesn't dispose the data reader either!
        let reader = execReader conn "select * from person" []
        let withReader reader =
            let all = reader
                      |> Seq.ofDataReader
                      |> LazyList.ofSeq
            all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r |> readInt "id").Value)
            all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r |> readString "name").Value)
        using reader withReader
    )
    ()