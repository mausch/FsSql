#r "bin\debug\FsSql.dll"
#r "bin\debug\System.Data.SQlite.dll"
#r "bin\debug\MySql.Data.dll"

open System
open System.Data

// a function that opens a connection
let openConn() =
    //let conn = new System.Data.SQLite.SQLiteConnection("Data Source=test.db;Version=3;New=True;")
    let conn = new MySql.Data.MySqlClient.MySqlConnection("server=localhost;user=root;database=fssqltest;port=3306;password=root;")
    conn.Open()
    conn :> IDbConnection

// the connection manager, encapsulates how to create and dispose the connection
let connMgr = Sql.withNewConnection openConn

// partial application of various common functions, around the connection manager
let sql = SqlWrapper(connMgr)
let exec a = sql.ExecNonQuery a [] |> ignore
let P = Sql.Parameter.make

// create the schema
exec "drop table if exists user"
exec "create table user (id int not null primary key, name varchar(255) not null, address varchar(255) null)"
exec "drop table if exists animal"
exec "create table animal (id int not null primary key, name varchar(255) not null, owner int null, animalType varchar(255) not null)"

// a function that inserts a record
let insertUser connMgr (id: int) (name: string) (address: string option) = 
    Sql.execNonQuery connMgr 
        "insert into user (id,name,address) values (@id,@name,@address)"
        [P("@id", id); P("@name", name); P("@address", address)]

// a function that inserts N records with some predefined values
let insertNUsers n conn =
    let insertUser = insertUser conn
    for i in 1..n do
        let name = sprintf "pepe %d" i
        let address = 
            if i % 2 = 0
                then None
                else Some (sprintf "fake st %d" i)
        insertUser i name address |> ignore

// wraps the n records insertion in a transaction
let insertNUsers2 n = insertNUsers n |> Tx.transactional

// executes the transaction, inserting 50 records
insertNUsers2 50 connMgr

let countUsers(): int64 =
    sql.ExecScalar "select count(*) from user" [] |> Option.get

printfn "%d users" (countUsers())

let printUser (dr: IDataRecord) =
    let id = (dr?id).Value
    let name = (dr?name).Value
    let address = 
        match dr?address with
        | None -> "No registered address"
        | Some x -> x
    printfn "Id: %d; Name: %s; Address: %s" id name address

sql.ExecReader "select * from user" []
|> Seq.ofDataReader
|> Seq.iter printUser

sql.ExecNonQueryF "delete from user where id > %d" 10 |> ignore

printfn "Now there are %d users" (countUsers()) // will print 10

// a record type representing a row in the table
type User = {
    id: int
    name: string
    address: string option
}

// maps a raw data record as a User record
let asUser (r: #IDataRecord) =
    {id = (r?id).Value; name = (r?name).Value; address = r?address}

// alternative, equivalent definition of asUser
let asUser2 r = Sql.asRecord<User> "" r

// get the first user
let firstUser = sql.ExecReader "select * from user limit 1" [] |> Sql.mapOne asUser 

printfn "first user's name: %s" firstUser.name
printfn "first user does%s have an address" (if firstUser.address.IsNone then " not" else "")

// a record type representing a row in the table
type Animal = {
    id: int
    name: string
    animalType: string
    owner: int option
}

let insertAnimal (animal: Animal) = 
    sql.ExecNonQuery
        "insert into animal (id, name, animalType, owner) values (@id, @name, @animalType, @owner)"
        (Sql.parameters ["@id",box animal.id; "@name",box animal.name; "@animalType",box animal.animalType; "@owner", box animal.owner])
        |> ignore

// inserting sample data
insertAnimal {id = 1; name = "Seymour"; animalType = "dog"; owner = Some 1}
insertAnimal {id = 2; name = "Goldie"; animalType = "fish"; owner = Some 1}
insertAnimal {id = 3; name = "Tramp"; animalType = "dog"; owner = None}

// mapping an inner join
let asUserWithAnimal (r: #IDataRecord) =
    Sql.asRecord<User> "u" r, Sql.asRecord<Animal> "a" r

let innerJoinSql = sprintf "select %s,%s from user u join animal a on a.owner = u.id" 
                      (Sql.recordFieldsAlias typeof<User> "u")
                      (Sql.recordFieldsAlias typeof<Animal> "a")

sql.ExecReader innerJoinSql []
|> Sql.map asUserWithAnimal
|> Seq.groupByFst
|> Seq.iter (fun (person, animals) ->
                printfn "%s has pets %s" person.name (String.Join(", ", animals |> Seq.map (fun a -> a.name))))


// mapping a left join
let asUserWithOptionalAnimal (r: #IDataRecord) =
    Sql.asRecord<User> "u" r, (Sql.asRecord<Animal> "a" |> Sql.optionalBy "a_id") r

let leftJoinSql = sprintf "select %s,%s from user u left join animal a on a.owner = u.id" 
                     (Sql.recordFieldsAlias typeof<User> "u")
                     (Sql.recordFieldsAlias typeof<Animal> "a")
sql.ExecReader leftJoinSql []
|> Sql.map asUserWithOptionalAnimal
|> Seq.groupByFst
|> Seq.chooseSnd
|> Seq.iter (fun (person, animals) ->
                printfn "%s has pets %s" person.name (String.Join(", ", animals |> Seq.map (fun a -> a.name))))

// clear tables
exec "delete from animal"
exec "delete from user"

// composable transactions
let txInsertUsers n = insertNUsers n |> Tx.required
let txInsertOneUser = Tx.mandatory insertUser
let insertAllUsers m = 
    txInsertUsers 10 m
    txInsertOneUser m 3 "John" None |> ignore
let txInsertAllUsers = Tx.required insertAllUsers
// run transaction, will fail due to duplicate PK
try
    txInsertAllUsers connMgr
with e ->
    printfn "Failed to insert all users:\n %s" e.Message
printfn "Now there are %d users" (countUsers()) // will print 0

// tx monad
let tx = Tx.TransactionBuilder()
let tran1() = tx {
    do! Tx.execNonQueryi
            "insert into user (id,name) values (@id,@name)"
            [P("@id", 99); P("@name", "John Doe")]
}
let tran() = tx {
    do! tran1()
    do! Tx.execNonQueryi "insert into blabla" [] // invalid SQL
    return 0
}
// execute transaction, will fail
match tran() connMgr with
| Tx.Commit a -> printfn "Transaction successful, return value %d" a
| Tx.Rollback a -> printfn "Transaction rolled back, return value %A" a
| Tx.Failed e -> printfn "Transaction failed with exception:\n %s" e.Message
printfn "Now there are %d users" (countUsers()) // will print 0