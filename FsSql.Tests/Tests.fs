module FsSql.Tests.FsSqlTests

open Fuchu
open System
open System.Collections.Generic
open System.Data
open System.Data.SQLite
open System.IO
open System.Linq
open System.Reflection
open FsSqlPrelude
open Microsoft.FSharp.Collections
open Microsoft.FSharp.Reflection
open FSharp.Collections.ParallelSeq
open FSharpx.Collections

let P = Sql.Parameter.make

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
    person: int
    street: string option
    city: string
}

type Person = {
    id: int
    name: string
    parent: int option
}

let createSchema conn =
    let exec a = Sql.execNonQuery (Sql.withConnection conn) a [] |> ignore
    exec "drop table if exists person"
    exec "create table person (id int primary key not null, name varchar not null, parent int null)"
    exec "drop table if exists address"
    exec "create table address (id int primary key not null, person int not null, street varchar null, city varchar not null)"
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

let withMemDb() =
    let conn = createConnectionAndSchema()
    Sql.withConnection conn

let userMapper r = 
    { id = (r?id).Value ; name = (r?name).Value; parent = r?parent }

let selectById conn = Sql.execReaderF conn "select * from person where id = %d"

let getUser conn id =
    selectById conn id |> Sql.mapOne userMapper

let findUser conn id =
    selectById conn id |> Sql.mapFirst userMapper

let insertUser conn (p: Person) =
    let param = Dictionary<string, obj>()
    param.["@id"] <- p.id
    param.["@name"] <- p.name
    Sql.execNonQuery conn
        "insert into person (id, name) values (@id, @name)"
        (Sql.paramsFromDict param)
        |> ignore
    //Sql.execNonQueryF conn "insert into person (id, name) values (%d, %s)" p.id p.name

let countUsers conn : int64 = 
    Sql.execScalar conn "select count(*) from person" [] |> Option.get

let countUsersAdoNet() : int64 = 
    use conn = createConnection()
    let cmd = conn.CreateCommand()
    cmd.CommandText <- "select count(*) from person"
    unbox cmd.ExecuteScalar

let deleteUser conn id = 
    Sql.execNonQueryF conn "delete person where id = %d" id |> ignore

let deleteUserAdoNet (id: int) = 
    use conn = createConnection()
    let cmd = conn.CreateCommand()
    cmd.CommandText <- "delete person where id = @id"
    let p = cmd.CreateParameter()
    p.ParameterName <- "@id"
    p.Value <- id
    cmd.Parameters.Add p |> ignore
    cmd.ExecuteNonQuery() |> ignore

let deleteUserWithCommand (id: int) = 
    let cmd = Sql.createCommand (Sql.withNewConnection createConnection)
    cmd.CommandText <- "delete person where id = @id"
    P("@id", id) |> Sql.addParameter cmd
    cmd.ExecuteNonQuery() |> ignore

let insertThenGet conn = 
    insertUser conn {id = 1; name = "pepe"; parent = None}
    Assert.Equal("user count", 1L, countUsers conn)
    let p = getUser conn 1
    Assert.Equal("user id", 1, p.id)
    Assert.Equal("user name", "pepe", p.name)

let findNonExistentRecord conn = 
    let p = findUser conn 39393
    Assert.None("user", p)

let findExistentRecord conn = 
    insertUser conn {id = 1; name = "pepe"; parent = None}
    let p = findUser conn 1
    match p with
    | None -> failtest "Expected Some x"
    | _ -> ()

let getMany conn = 
    for i in 1..50 do
        insertUser conn {id = i; name = "pepe" + i.ToString(); parent = None}
    let first10 = Sql.execReaderF conn "select * from person" |> Seq.ofDataReader |> Seq.truncate 10
    for i in first10 do
        logf "%d\n" (i?id).Value

let someTranAndFail conn =
    insertUser conn {id = 1; name = "pepe"; parent = None}
    insertUser conn {id = 2; name = "jose"; parent = None}
    failwith "Bla"

let transactionWithException conn =
    let someTran = Tx.transactional someTranAndFail
    match someTran conn with
    | Tx.TxResult.Failed _ -> ()
    | x -> failtestf "Expected tx failure, actual %A" x

    Assert.Equal("user count", 0L, countUsers conn)

let someTran conn =
    insertUser conn {id = 1; name = "pepe"; parent = None}
    insertUser conn {id = 2; name = "jose"; parent = None}

let transactionCommitted conn =
    let someTran conn = someTran conn
    let someTran = Tx.transactional someTran
    match someTran conn with
    | Tx.TxResult.Commit () -> ()
    | x -> failtestf "Expected tx success, actual %A" x
    Assert.Equal("user count", 2L, countUsers conn)

let someTranWithSubTran conn =
    let subtran conn = 
        insertUser conn {id = 3; name = "jorge"; parent = None}
        failwith "this fails"
    (Tx.transactional subtran) conn |> ignore
    insertUser conn {id = 1; name = "pepe"; parent = None}
    insertUser conn {id = 2; name = "jose"; parent = None}
    ()

let nestedTransactionsAreNotSupported conn =
    let someTran = Tx.transactional someTranWithSubTran >> ignore
    Assert.Raise("transaction exception", typeof<SQLiteException>, fun _ -> someTran conn)

let transactionWithOption conn =
    let someTran = Tx.transactional someTranAndFail
    let result = someTran conn
    match result with
    | Tx.Commit v -> failtestf "Expected transaction fail, actual Commit %A" v
    | Tx.Rollback v -> failtestf "Expected transaction fail, actual Rollback %A" v
    | Tx.Failed e -> () //printfn "Failed with exception %A" e
    Assert.Equal("user count", 0L, countUsers conn)

// Tests whether n is prime - expects n > 1
// From http://tomasp.net/blog/fsparallelops.aspx
let isPrime n =
    // calculate how large divisors should we test..
    let max = int (sqrt (float n))
    // try to divide n by 2..max (stops when divisor is found)
    not ({ 2..max } |> Seq.filter (fun d -> n%d = 0) |> Enumerable.Any)

let ``pseq isprime`` () =
    let p = {100000..800000}
            |> PSeq.filter isPrime
            |> PSeq.length
    Assert.Equal("prime count", 54359, p)

let insertUsers conn =
    log "inserting"
    let insert conn =
        //for i in 100000000..100050000 do
        for i in 1..10 do
            insertUser conn {id = i; name = "pepe" + i.ToString(); parent = None}
    let insert = Tx.transactionalWithIsolation IsolationLevel.ReadCommitted insert
    insert conn
    
let dataReaderIsParallelizable conn =
    insertUsers conn |> ignore
    log "reading"
    let primes = Sql.execReader conn "select * from person" []
                    |> Seq.ofDataReader
                    |> PSeq.map (fun r -> (r?id).Value)
                    |> PSeq.filter isPrime
                    |> PSeq.length
    logf "%d primes" primes

let dataReaderToSeqIsForwardOnly conn =
    insertUsers conn |> ignore
    let all = Sql.execReader conn "select * from person" []
                |> Seq.ofDataReader
    all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "id: %d\n" (r?id).Value)
    let secondIter() = 
        all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "name: %s\n" (r?name).Value)
    Assert.Raise("invalid operation", typeof<InvalidOperationException>, secondIter)

let dataReaderToSeqIsCacheable conn =
    insertUsers conn |> ignore
    // this doesn't dispose the data reader
    let all = Sql.execReader conn "select * from person" []
                |> Seq.ofDataReader
                |> Seq.cache
    all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "id: %d\n" (r?id).Value)
    all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "name: %s\n" (r?name).Value)

let dataReaderToSeqIsCacheable2 conn =
    insertUsers conn |> ignore
    use reader = Sql.execReader conn "select * from person" []
    let all = reader
                |> Seq.ofDataReader
                |> Seq.cache
    all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "id: %d\n" (r?id).Value)
    all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "name: %s\n" (r?name).Value)

let dataReaderToSeqIsCacheable3 conn =
    insertUsers conn |> ignore
    let reader = Sql.execReader conn "select * from person" []
    let withReader reader =
        let all = reader
                    |> Seq.ofDataReader
                    |> Seq.cache
        all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "id: %d\n" (r?id).Value)
        all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "name: %s\n" (r?name).Value)
    using reader withReader

let dataReaderWithLazyList conn =
    insertUsers conn |> ignore
    let reader = Sql.execReader conn "select * from person" []
    let withReader reader =
        let all = reader
                    |> Seq.ofDataReader
                    |> LazyList.ofSeq
        all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "id: %d\n" (r?id).Value)
        all |> Seq.truncate 10 |> Seq.iter (fun r -> logf "name: %s\n" (r?name).Value)
    using reader withReader

let ``create command`` () = 
    let cmgr = withNewDbFile()
    use cmd = Sql.createCommand cmgr
    cmd.CommandText <- "select * from person where id < @id"
    [P("@id", 0)] |> Seq.iter (Sql.addParameter cmd)
    let r = cmd.ExecuteReader() |> List.ofDataReader
    Assert.Equal("result count", 0, r.Length)

let ``async exec reader`` () = 
    let cmgr = withNewDbFile()
    async {
        use! reader = Sql.asyncExecReader cmgr "select * from person where id < @id" [P("@id", 0)]
        let r = reader |> List.ofDataReader
        Assert.Equal("result count", 0, r.Length)
    } |> Async.RunSynchronously


let ``duplicate field names are NOT supported``() =
    let c = withNewDbFile()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    Sql.execNonQueryF c "insert into address (id, street, city, person) values (%d, %s, %s, %d)" 1 "fake st" "NY" 5 |> ignore
    use reader = Sql.execReader c "select * from person p join address a on a.person = p.id" []
    let readReader () = List.ofDataReader reader
    Assert.Raise("", typeof<ArgumentException>, readReader >> ignore)

let asAddress (r: IDataRecord) =
    {id = (r?a_id).Value; street = r?a_street; city = (r?a_city).Value; person = (r?a_person).Value }

let asPerson (r: IDataRecord) =
    {id = (r?p_id).Value; name = (r?p_name).Value; parent = r?p_parent}


let ``inner join``() =
    let c = withNewDbFile()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    let insertAddress = Sql.execNonQueryF c "insert into address (id, person, street, city) values (%d, %d, %s, %s)"
    insertAddress 1 5 "fake st" "NY" |> ignore
    insertAddress 2 5 "another address" "CO" |> ignore

    let personFields = Sql.recordFieldsAlias typeof<Person> "p"
    let addressFields = Sql.recordFieldsAlias typeof<Address> "a"
    let sql = sprintf "select %s,%s from person p join address a on p.id = a.person" personFields addressFields
    logf "%s\n" sql

    let asPersonWithAddresses (r: IDataRecord) =
        asPerson r, asAddress r

    let records = Sql.execReader c sql []
                    |> Sql.map asPersonWithAddresses 
                    |> Seq.cache 
                    |> Seq.groupByFst 
                    |> List.ofSeq

    Assert.Equal("result count", 1, records.Length)
    let person,addresses = records.[0]
    Assert.Equal("person id", 5, person.id)
    Assert.Equal("address count", 2, Seq.length addresses)

let ``left join``() =
    let c = withNewDbFile()

    let insertPerson = Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)"
    insertPerson 5 "John" |> ignore
    insertPerson 6 "George" |> ignore

    let insertAddress = Sql.execNonQueryF c "insert into address (id, person, street, city) values (%d, %d, %s, %s)"
    insertAddress 1 5 "fake st" "NY" |> ignore
    insertAddress 2 5 "another address" "CO" |> ignore

    let personFields = Sql.recordFieldsAlias typeof<Person> "p"
    let addressFields = Sql.recordFieldsAlias typeof<Address> "a"
    let sql = sprintf "select %s,%s from person p left join address a on p.id = a.person order by p.id" personFields addressFields
    logf "%s\n" sql

    let asPersonWithAddresses (r: IDataRecord) =
        asPerson r, (asAddress |> Sql.optionalBy "a_id") r

    let records = Sql.execReader c sql [] 
                    |> Sql.map asPersonWithAddresses 
                    |> Seq.cache 
                    |> Seq.groupByFst 
                    |> Seq.chooseSnd
                    |> List.ofSeq

    Assert.Equal("record count", 2, records.Length)
    Assert.Equal("first id", 5, (fst records.[0]).id)
    Assert.Equal("second id", 6, (fst records.[1]).id)
    Assert.Equal("first address count", 2, Seq.length (snd records.[0]))
    Assert.Equal("second address count", 0, Seq.length (snd records.[1]))

let ``list of map`` ()=
    let c = withNewDbFile()
    let insertPerson = Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)"
    insertPerson 5 "John" |> ignore
    use reader = Sql.execReader c "select * from person" []
    let keys = reader |> Sql.map Sql.asMap |> Seq.head |> Seq.map ((|KeyValue|) >> fst) |> List.ofSeq
    Assert.Equal("", "id", keys.[0])
    Assert.Equal("", "name", keys.[1])
    Assert.Equal("", "parent", keys.[2])

let ``map asRecord`` () = 
    let c = withNewDbFile()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    let p = Sql.execReader c "select id,name,parent from person where id = 5" [] |> Sql.map (Sql.asRecord<Person> "") |> Enumerable.First
    Assert.Equal("id", 5, p.id)
    Assert.Equal("name", "John", p.name)
    Assert.None("parent", p.parent)

let ``map asRecord with different field order`` () = 
    let c = withNewDbFile()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    let p = Sql.execReader c "select name,id,parent from person where id = 5" [] |> Sql.map (Sql.asRecord<Person> "") |> Enumerable.First
    Assert.Equal("id", 5, p.id)
    Assert.Equal("name", "John", p.name)
    Assert.None("parent", p.parent)

let ``map asRecord with too few fields throws`` () =
    let c = withNewDbFile()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    use reader = Sql.execReader c "select name,id from person where id = 5" []
    let proc () = reader |> Sql.map (Sql.asRecord<Person> "") |> Enumerable.First |> ignore
    Assert.Raise("", typeof<TargetParameterCountException>, proc)

let ``map asRecord with prefix`` () = 
    let c = withNewDbFile()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    let sql = sprintf "select %s from Person p where id = 5" (Sql.recordFieldsAlias typeof<Person> "p")
    let p = Sql.execReader c sql [] |> Sql.map (Sql.asRecord<Person> "p") |> Enumerable.First
    Assert.Equal("id", 5, p.id)
    Assert.Equal("name", "John", p.name)
    Assert.None("parent", p.parent)

let ``map asRecord with prefix with more fields`` () = 
    let c = withNewDbFile()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    Sql.execNonQueryF c "insert into address (id, person, street, city) values (%d, %d, %s, %s)" 2 5 "Fake st" "NY" |> ignore
    let sql = sprintf "select %s, a.* from Person p, address a where p.id = 5" (Sql.recordFieldsAlias typeof<Person> "p")
    let p = Sql.execReader c sql [] |> Sql.map (Sql.asRecord<Person> "p") |> Enumerable.First
    Assert.Equal("id", 5, p.id)
    Assert.Equal("name", "John", p.name)
    Assert.None("parent", p.parent)
        
let ``asRecord throws with non-record type`` () = 
    Assert.Raise("", typeof<ArgumentException>, fun _ -> Sql.asRecord<string> |> ignore)

let ``map single field`` () =
    let c = withMemDb()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    let v = Sql.execReader c "select id from person" [] |> Sql.map (Sql.asScalari<int> 0)
    let v = Seq.toList v
    Assert.Equal("result count", 1, v.Length)
    Assert.Equal("id", 5, v.[0])

let ``map single field as option`` () =
    let c = withMemDb()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    let v = Sql.execReader c "select id from person" [] |> Sql.map (Sql.asScalari<int option> 0)
    let v = Seq.toList v
    Assert.Equal("result count", 1, v.Length)
    Assert.Equal("id", Some 5, v.[0])

let ``map to pair`` () =
    let c = withMemDb()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    let v = Sql.execReader c "select id,name from person" [] |> Sql.map Sql.asPair
    let v = Seq.toList v
    Assert.Equal("result count", 1, v.Length)
    Assert.Equal("id", 5, fst v.[0])
    Assert.Equal("name", "John", snd v.[0])

let ``map to triple`` () =
    let c = withMemDb()
    Sql.execNonQueryF c "insert into person (id, name) values (%d, %s)" 5 "John" |> ignore
    let v = Sql.execReader c "select id,name,parent from person" [] |> Sql.map Sql.asTriple<int,string,int option>
    let v = Seq.toList v
    Assert.Equal("result count", 1, v.Length)
    let id,name,parent = v.[0]
    Assert.Equal("id", 5, id)
    Assert.Equal("name", "John", name)
    Assert.None("parent", parent)

let ``compose tx`` () = 
    let c = withNewDbFile()
    let f1 m = Sql.execNonQueryF m "insert into person (id,name) values (%d, %s)" 3 "one" |> ignore
    let f1 = Tx.required f1
    let f2 m = Sql.execNonQueryF m "invalid sql statement" |> ignore
    let f2 = Tx.supported f2
    let finalTx mgr =
        f1 mgr |> ignore
        f2 mgr
    try
        (Tx.required finalTx) c |> ignore
        failtest "Tx should have failed"
    with e -> 
        logf "%s\n" e.Message
        Assert.Equal("user count", 0L, countUsers c)

let ``tx required and never throw`` () = 
    let c = withNewDbFile()
    let f1 m = Sql.execNonQueryF m "insert into person (id,name) values (%d, %s)" 3 "one" |> ignore
    let f1 = f1 |> Tx.never |> Tx.required
    try
        f1 c |> ignore
        failtest "Tx should have failed"
    with e -> 
        logf "%s\n" e.Message
        Assert.Equal("user count", 0L, countUsers c)

let tx = Tx.TransactionBuilder()

let ``tx monad error`` () = 
    let c = withMemDb()
    let v = ref false
    let tran = tx {
        let! x = Tx.execNonQuery "select" []
        v := true
        return 3
    }
    let result = tran c // execute transaction
    match result with
    | Tx.Failed e -> 
        Assert.Equal("executed", false, !v)
        logf "Error: %A\n" e
    | _ -> failtest "Transaction should have failed"

let txInsert id = 
    Tx.execNonQueryi "insert into person (id,name) values (@id, @name)" [P("@id",id);P("@name", "juan")]

let ``tx monad error rollback`` () = 
    let c = withMemDb()
    let tran = tx {
        let! x = Tx.execNonQuery "insert into person (id,name) values (@id, @name)" [P("@id",3);P("@name", "juan")]
        let! x = Tx.execNonQuery "select" []
        return ()
    }
    let result = tran c // execute transaction
    match result with
    | Tx.Failed e -> 
        logf "Error: %A\n" e
        Assert.Equal("user count", 0L, countUsers c)
    | _ -> failtest "Transaction should have failed"

let ``tx monad ok`` () = 
    let c = withMemDb()
    let tran = tx {
        do! txInsert 3 
        do! txInsert 4
        return 8
    }
    let result = tran c // execute transaction
    match result with
    | Tx.Commit a -> Assert.Equal("transaction result", 8, a)
    | Tx.Failed e -> failtest "Transaction should not have failed"
    | Tx.Rollback e -> failtest "Transaction should not have failed"
    Assert.Equal("user count", 2L, countUsers c)

let ``tx monad using`` () = 
    let c = withMemDb()
    let tran = tx {
        do! txInsert 3
        use! reader = Tx.execReader "select * from person" []
        let id = 
            reader 
            |> Seq.ofDataReader 
            |> Seq.map (Sql.readField "id" >> Option.get)
            |> Enumerable.First
        return id
    }
    let result = tran c
    match result with
    | Tx.Commit a -> Assert.Equal("transaction result", 3, a)
    | Tx.Rollback a -> failtest "Transaction should not have failed"
    | Tx.Failed e -> failtest "Transaction should not have failed"

let ``tx monad rollback and zero`` () = 
    let c = withMemDb()
    let tran = tx {
        do! txInsert 3
        if 1 = 1
            then do! Tx.rollback 4
        return 0
    }
    let result = tran c
    match result with
    | Tx.Rollback a -> 
        Assert.Equal("rollback result", 4, a)
        Assert.Equal("user count", 0L, countUsers c)
    | _ -> failtest "Transaction should have been rolled back"

let ``tx monad tryfinally`` () = 
    let c = withMemDb()
    let finallyRun = ref false
    let tran = tx {
        try
            do! txInsert 3
            failwith "Error!"
        finally
            finallyRun := true
    }
    let result = tran c
    match result with
    | Tx.Failed e ->
        Assert.Equal("fail message", "Error!", e.Message)
        Assert.Equal("finally run", true, !finallyRun)
    | _ -> failtest "Transaction should have failed"

let ``tx monad composable`` () =
    let c = withMemDb()
    let tran1 = tx {
        do! txInsert 3
    }
    let tran = tx {
        do! tran1
        do! txInsert 4
    }
    let result = tran c
    match result with
    | Tx.Commit a -> Assert.Equal("user count", 2L, countUsers c)
    | Tx.Rollback a -> failtest "Transaction should not have failed"
    | Tx.Failed e -> failtest "Transaction should not have failed"

let ``tx monad for`` () = 
    let c = withMemDb()
    let tran = tx {
        for i in 1..50 do
            do! txInsert i
    }
    let result = tran c
    match result with
    | Tx.Commit a -> Assert.Equal("user count", 50L, countUsers c)
    | Tx.Rollback a -> failtest "Transaction should not have failed"
    | Tx.Failed e -> failtest "Transaction should not have failed"

let ``tx monad for with error`` () =
    let c = withMemDb()
    let tran = tx {
        for i in 1..50 do
            do! txInsert 1
    }
    let result = tran c
    match result with
    | Tx.Failed e -> Assert.Equal("user count", 0L, countUsers c)
    | _ -> failtest "Transaction should have failed"

(*
let ``tx monad while`` () =
    let c = withMemDb()
    let tran() = tx {
        let i = ref 0
        while !i < 50 do
            printfn "%d" !i
            do! Tx.execNonQueryi "insert into person (id,name) values (@id, @name)" [P("@id",i);P("@name", "juan")]
            incr i
    }
    let result = tran() c
    match result with
    | Tx.Commit a -> Assert.AreEqual(50L, countUsers c)
    | Tx.Rollback a -> failwith "Transaction should not have failed"
    | Tx.Failed e -> failwithe e "Transaction should not have failed"
*)

let ``tx monad trywith``() =
    let c = withMemDb()
    let tran = tx {
        try
            do! txInsert 1
            failwith "bye"
            do! txInsert 2
        with e ->
            do! txInsert 3
    }
    let result = tran c
    match result with
    | Tx.Commit a -> Assert.Equal("user count", 2L, countUsers c)
    | Tx.Rollback a -> failtest "Transaction should not have failed"
    | Tx.Failed e -> failtest "Transaction should not have failed"

let ``tx then no tx``() =
    let c = withMemDb()
    let t = txInsert 1 |> Tx.required >> Tx.get
    t c |> ignore
    let l = Sql.execReader c "select * from person" [] |> List.ofDataReader
    Assert.Equal("result count", 1, l.Length)

let ``can execute serialisable tx``() =
      let cm = withMemDb()
      let select_one m = Sql.execScalar m "select 1" []
      let select_one =
        select_one |> Tx.transactionalWithIsolation IsolationLevel.Serializable
      let res = select_one cm
      Assert.Equal("Can execute tx", Tx.TxResult.Commit(Some 1L), res)

let ``can execute serialisable tx 2``() =
    let tx = Tx.TransactionBuilder(IsolationLevel.Serializable)
    let cm = withMemDb()
    let select_one = tx {
        logf "executing 1"
        let! a = Tx.execScalar "select 1" []
        let a : int64 = Option.get a
        logf "rolling back"
        do! Tx.rollback 3L
        logf "executing 2"
        let! b = Tx.execScalar "select 1" []
        let b : int64 = Option.get b
        return a + b
    }
    let res = select_one cm
    Assert.Equal("Can execute tx", Tx.TxResult.Rollback 3L, res)

// define test cases

let connMgrTests = 
    [
        insertThenGet, "insert then get"
        findNonExistentRecord, "find non existent record"
        findExistentRecord, "find existent record"
        getMany, "select all truncate 10"
        transactionWithException, "transaction with exception"
        transactionCommitted, "transaction committed"
        nestedTransactionsAreNotSupported, "nested transactions are not supported"
        transactionWithOption, "transaction with option"
        dataReaderIsParallelizable, "datareader is parallelizable"
        dataReaderToSeqIsForwardOnly, "datareader to seq is forward only"
        //dataReaderToSeqIsCacheable, "dataReaderToSeqIsCacheable"
        //dataReaderToSeqIsCacheable2, "dataReaderToSeqIsCacheable2"
        //dataReaderToSeqIsCacheable3, "dataReaderToSeqIsCacheable3"
        dataReaderWithLazyList, "datareader with lazy list"
    ]

open Fuchu

let genTests conn suffix = 
    [ for test,name in connMgrTests ->
        testCase (name + " " + suffix) (fun () -> conn() |> test) ]

let persistentDBTests = genTests withNewDbFile "(file db)"
let memDBTests = genTests withMemDb "(memory db)"

let otherParallelizableTests = 
    [
        "tx then no tx", ``tx then no tx``
        "tx monad trywith", ``tx monad trywith``
        "tx monad for with error", ``tx monad for with error``
        "tx monad for", ``tx monad for``
        "tx monad composable", ``tx monad composable``
        "tx monad tryfinally", ``tx monad tryfinally``
        "tx monad rollback and zero", ``tx monad rollback and zero``
        "tx monad using", ``tx monad using``
        "tx monad ok", ``tx monad ok``
        "tx monad error rollback", ``tx monad error rollback``
        "tx monad error", ``tx monad error``
        "map to pair", ``map to pair``
        "map single field as option", ``map single field as option``
        "map single field", ``map single field``
        "asRecord throws with non-record type", ``asRecord throws with non-record type``
        "can execute serialisable tx", ``can execute serialisable tx``
        "can execute serialisable tx 2", ``can execute serialisable tx 2``
    ] |> List.map (fun (name, f) -> Fuchu.Tests.testCase name f)

let nonParallelizableTests = 
    [
        ``tx required and never throw``
        ``compose tx``
        ``map to triple``
        ``map asRecord with prefix with more fields``
        ``map asRecord with prefix``
        ``map asRecord with too few fields throws``
        ``map asRecord with different field order``
        ``map asRecord``
        ``list of map``
        ``left join``
        ``inner join``
        ``duplicate field names are NOT supported``
        ``async exec reader``
        ``create command``
    ] |> List.map TestCase

