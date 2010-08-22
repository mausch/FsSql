module FsSql.Tests

open MbUnit.Framework
open System
open System.Data
open System.Data.SQLite
open System.IO
open System.Linq
open FsSqlPrelude
open Microsoft.FSharp.Collections
open Microsoft.FSharp.Reflection

[<Test>]
let catchtest() =
    let f a b = a/b
    let g = catch 7 (f 10)
    let x = g 0
    Assert.AreEqual(7, x)
    assertThrows<DivideByZeroException>(fun () -> f 10 0 |> ignore)
    ()

let createConnection() =
    let conn = new System.Data.SQLite.SQLiteConnection("Data Source=:memory:;Version=3;New=True")
    conn.Open()
    conn :> IDbConnection

let createPersistentConnection() =
    let conn = new System.Data.SQLite.SQLiteConnection("Data Source=test.db;Version=3;New=True;Pooling=false;Max Pool Size=0;")
    conn.Open()
    conn :> IDbConnection

type Address = {
    id: int
    street: string option
    city: string
}

type Person = {
    id: int
    name: string
    address: Address option
}

let createSchema conn =
    let exec a = Sql.execNonQuery (Sql.withConnection conn) a [] |> ignore
    exec "drop table if exists person"
    exec "create table person (id int primary key not null, name varchar not null, address int null)"
    exec "drop table if exists address"
    exec "create table address (id int primary key not null, street varchar null, city varchar not null)"
    log "done creating schema"

let createConnectionAndSchema() =
    let conn = createConnection()
    createSchema conn
    conn

let withDatabase f = 
    let createConnectionAndSchema() =
        let conn = createConnection()
        createSchema conn
        conn
    withResource createConnectionAndSchema (fun c -> c.Dispose()) f

let withPersistentDatabase f = 
    let createConnectionAndSchema() =
        let conn = createPersistentConnection()
        createSchema conn
        conn
    withResource createConnectionAndSchema (fun c -> c.Dispose()) f

File.Delete "test.db"

let withNewDbFile() =    
    createSchema (createPersistentConnection())
    Sql.withNewConnection createPersistentConnection

let userMapper r = 
    { id = (r?id).Value ; name = (r?name).Value; address = None}

let selectById conn = Sql.execReaderF conn "select * from person where id = %d"

let getUser conn id =
    selectById conn id |> Sql.mapOne userMapper

let findUser conn id =
    selectById conn id |> Sql.mapFirst userMapper

let insertUser conn (p: Person) =
    Sql.execNonQuery conn
        "insert into person (id, name) values (@id, @name)"
        (Sql.parameters ["@id", box p.id; "@name", box p.name])
        |> ignore
    //Sql.execNonQueryF conn "insert into person (id, name) values (%d, %s)" p.id p.name

let updateUser conn (p: Person) =
    Sql.execNonQueryF conn "update person set name = %s where id = %d" p.name p.id

let countUsers conn : int64 = 
    Sql.execScalar conn "select count(*) from person" []

let deleteUser conn id = 
    Sql.execNonQueryF conn "delete person where id = %d" id |> ignore

let insertThenGet conn = 
    insertUser conn {id = 1; name = "pepe"; address = None}
    printfn "count: %d" (countUsers conn)
    let p = getUser conn 1
    printfn "id=%d, name=%s" p.id p.name

[<Test;Parallelizable>]
let ``insert then get``() = 
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        insertThenGet conn)
    ()

[<Test>]
let ``insert then get persistent`` () = 
    insertThenGet (withNewDbFile())

let findNonExistentRecord conn = 
    let p = findUser conn 39393
    Assert.IsTrue p.IsNone
    printfn "end test"

[<Test;Parallelizable>]
let ``find non-existent record``() =
    withDatabase (fun conn ->
        findNonExistentRecord (Sql.withConnection conn))

[<Test>]
let ``find non-existent record persistent``() =
    findNonExistentRecord (withNewDbFile())

let findExistentRecord conn = 
    insertUser conn {id = 1; name = "pepe"; address = None}
    let p = findUser conn 1
    Assert.IsTrue p.IsSome
    printfn "end test"

[<Test;Parallelizable>]
let ``find existent record``() =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        findExistentRecord conn)

[<Test>]
let ``find existent record persistent``() =
    findExistentRecord (withNewDbFile())

let getMany conn = 
    for i in 1..50 do
        insertUser conn {id = i; name = "pepe" + i.ToString(); address = None}
    let first10 = Sql.execReaderF conn "select * from person" |> Seq.ofDataReader |> Seq.truncate 10
    for i in first10 do
        printfn "%d" (i?id).Value
    printfn "end!"

[<Test;Parallelizable>]
let ``get many``() =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        getMany conn)


[<Test>]
let ``get many persistent``() =
    getMany (withNewDbFile())

let someTranAndFail conn =
    insertUser conn {id = 1; name = "pepe"; address = None}
    insertUser conn {id = 2; name = "jose"; address = None}
    failwith "Bla"
    ()

let transactionWithException conn =
    let someTran = Sql.transactional conn someTranAndFail
    let someTran = catch () someTran
    someTran()
    Assert.AreEqual(0L, countUsers conn)

[<Test;Parallelizable>]
let ``transaction with exception`` () =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        transactionWithException conn)
    ()

[<Test>]
let ``transaction with exception persistent``() =
    transactionWithException (withNewDbFile())

let someTran conn =
    insertUser conn {id = 1; name = "pepe"; address = None}
    insertUser conn {id = 2; name = "jose"; address = None}
    ()

let transactionCommitted conn =
    let someTran conn = someTran conn
    let someTran = Sql.transactional conn someTran
    someTran()
    Assert.AreEqual(2L, countUsers conn)

[<Test;Parallelizable>]
let ``transaction committed`` () =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        transactionCommitted conn)
    ()

[<Test>]
let ``transaction committed persistent``() =
    transactionCommitted (withNewDbFile())

let someTranWithSubTran conn =
    let subtran conn = 
        insertUser conn {id = 3; name = "jorge"; address = None}
        failwith "this fails"
    (Sql.transactional conn subtran)()
    insertUser conn {id = 1; name = "pepe"; address = None}
    insertUser conn {id = 2; name = "jose"; address = None}
    ()

let nestedTransactionsAreNotSupported conn =
    let someTran = Sql.transactional conn someTranWithSubTran
    assertThrows<SQLiteException> someTran

[<Test;Parallelizable>]
let ``nested transactions are NOT supported`` () =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        nestedTransactionsAreNotSupported conn)
    ()

[<Test>]
let ``nested transactions are NOT supported persistent`` () =
    nestedTransactionsAreNotSupported (withNewDbFile())

let transactionWithOption conn =
    let someTran = Sql.transactional2 conn someTranAndFail
    let result = someTran()
    match result with
    | Sql.Success v -> raise <| Exception("transaction should have failed!")
    | Sql.Failure e -> printfn "Failed with exception %A" e
    Assert.AreEqual(0L, countUsers conn)

[<Test;Parallelizable>]
let ``transaction with option`` () =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        transactionWithOption conn)

[<Test>]
let ``transaction with option persistent``() =
    transactionWithOption (withNewDbFile())

// Tests whether n is prime - expects n > 1
// From http://tomasp.net/blog/fsparallelops.aspx
let isPrime n =
    // calculate how large divisors should we test..
    let max = int (sqrt (float n))
    // try to divide n by 2..max (stops when divisor is found)
    not ({ 2..max } |> Seq.filter (fun d -> n%d = 0) |> Enumerable.Any)

[<Test;Parallelizable>]
let ``pseq isprime`` () =
    let p = {100000..800000}
            |> PSeq.filter isPrime
            |> PSeq.length

    printfn "%d primes" p
    ()

let insertUsers conn =
    log "inserting"
    let insert conn =
        //for i in 100000000..100050000 do
        for i in 1..10 do
            insertUser conn {id = i; name = "pepe" + i.ToString(); address = None}
    let insert = Sql.transactionalWithIsolation IsolationLevel.ReadCommitted conn insert
    insert()
    
let dataReaderIsParallelizable conn =
    insertUsers conn
    log "reading"
    let primes = Sql.execReader conn "select * from person" []
                    |> Seq.ofDataReader
                    |> Seq.map (fun r -> (r?id).Value)
                    |> PSeq.filter isPrime
                    |> PSeq.length
    logf "%d primes" primes


[<Test;Parallelizable>]
let ``datareader is parallelizable`` () =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        dataReaderIsParallelizable conn)

[<Test>]
let ``datareader is parallelizable persistent``() =
    dataReaderIsParallelizable(withNewDbFile())

let dataReaderToSeqIsForwardOnly conn =
    insertUsers conn
    let all = Sql.execReader conn "select * from person" []
                |> Seq.ofDataReader
    all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r?id).Value)
    let secondIter() = 
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r?name).Value)
    assertThrows<InvalidOperationException> secondIter

[<Test;Parallelizable>]
let ``datareader to seq is forward-only``() =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        dataReaderToSeqIsForwardOnly conn)

[<Test>]
let ``datareader to seq is forward-only persistent``() =
    dataReaderToSeqIsForwardOnly (withNewDbFile())


let dataReaderToSeqIsCacheable conn =
    insertUsers conn
    // this doesn't dispose the data reader
    let all = Sql.execReader conn "select * from person" []
                |> Seq.ofDataReader
                |> Seq.cache
    all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r?id).Value)
    all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r?name).Value)

[<Test;Parallelizable>]
let ``datareader to seq is cacheable`` () =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        dataReaderToSeqIsCacheable conn)

[<Test;Ignore("locks the database because it doesn't close the connection")>]
let ``datareader to seq is cacheable persistent``() =
    dataReaderToSeqIsCacheable (withNewDbFile())

let dataReaderToSeqIsCacheable2 conn =
    insertUsers conn
    use reader = Sql.execReader conn "select * from person" []
    let all = reader
                |> Seq.ofDataReader
                |> Seq.cache
    all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r?id).Value)
    all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r?name).Value)

[<Test;Parallelizable>]
let ``datareader to seq is cacheable 2`` () =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        dataReaderToSeqIsCacheable2 conn)


[<Test>]
let ``datareader to seq is cacheable 2 persistent`` () =
    dataReaderToSeqIsCacheable2 (withNewDbFile())


let dataReaderToSeqIsCacheable3 conn =
    insertUsers conn
    let reader = Sql.execReader conn "select * from person" []
    let withReader reader =
        let all = reader
                    |> Seq.ofDataReader
                    |> Seq.cache
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r?id).Value)
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r?name).Value)
    using reader withReader

[<Test;Parallelizable>]
let ``datareader to seq is cacheable 3`` () =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        dataReaderToSeqIsCacheable3 conn)

[<Test>]
let ``datareader to seq is cacheable 3 persistent`` () =
    dataReaderToSeqIsCacheable3 (withNewDbFile())

let dataReaderWithLazyList conn =
    insertUsers conn
    let reader = Sql.execReader conn "select * from person" []
    let withReader reader =
        let all = reader
                    |> Seq.ofDataReader
                    |> LazyList.ofSeq
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "id: %d" (r?id).Value)
        all |> Seq.truncate 10 |> Seq.iter (fun r -> printfn "name: %s" (r?name).Value)
    using reader withReader

[<Test;Parallelizable>]
let ``datareader with lazylist`` () =
    withDatabase (fun conn ->
        let conn = Sql.withConnection conn
        dataReaderWithLazyList conn)

[<Test>]
let ``datareader with lazylist persistent`` () =
    dataReaderWithLazyList (withNewDbFile())

[<Test>]
let ``duplicate field names are NOT supported``() =
    let c = withNewDbFile()
    Sql.execNonQueryF c "insert into address (id, street, city) values (%d, %s, %s)" 1 "fake st" "NY" |> ignore
    Sql.execNonQueryF c "insert into person (id, name, address) values (%d, %s, %d)" 5 "John" 1 |> ignore
    use reader = Sql.execReader c "select * from person p join address a on a.id = p.address" []
    assertThrows<ArgumentException> (fun () -> reader |> List.ofDataReader |> ignore)
    ()

[<Test>]
let ``inner join``() =
    let c = withNewDbFile()
    Sql.execNonQueryF c "insert into address (id, street, city) values (%d, %s, %s)" 1 "fake st" "NY" |> ignore
    Sql.execNonQueryF c "insert into person (id, name, address) values (%d, %s, %d)" 5 "John" 1 |> ignore
    let recordFields t = FSharpType.GetRecordFields t |> Array.map (fun p -> p.Name)
    let fieldAlias alias = Array.map (fun s -> sprintf "%s.%s %s_%s" alias s alias s)
    let sjoin (sep: string) (strings: string[]) = String.Join(sep, strings)
    let recordFieldsAlias ty alias = recordFields ty |> fieldAlias alias |> sjoin ","
    let personFields = recordFieldsAlias typeof<Person> "p"
    let addressFields = recordFieldsAlias typeof<Address> "a"
    let sql = sprintf "select %s,%s from person p join address a on a.id = p.address" personFields addressFields
    printfn "%s" sql

    let asAddress (r: IDataRecord) =
        {id = (r?a_id).Value; street = r?a_street; city = (r?a_city).Value }
    let asAddressOption = asAddress |> Sql.optionalBy "a_id"
    let asPerson (r: IDataRecord) =
        {id = (r?p_id).Value; name = (r?p_name).Value; address = asAddressOption r}

    let records = Sql.execReader c sql [] |> Sql.map asPerson |> List.ofSeq
    Assert.AreEqual(1, records.Length)
    let person = records.[0]
    Assert.AreEqual(5, person.id)
    Assert.IsTrue person.address.IsSome
    Assert.AreEqual("fake st", person.address.Value.street.Value)
    ()