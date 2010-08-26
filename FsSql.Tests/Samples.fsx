#r "bin\debug\FsSql.dll"
#r "bin\debug\System.Data.SQlite.dll"

open System.Data

// a function that opens a connection
let openConn() =
    let conn = new System.Data.SQLite.SQLiteConnection("Data Source=test.db;Version=3;New=True;")
    conn.Open()
    conn :> IDbConnection

// the connection manager, encapsulates how to create and dispose the connection
let connMgr = Sql.withNewConnection openConn

// partial application of various common functions, around the connection manager
let inTransaction a = Sql.transactional connMgr a
let execScalar sql = Sql.execScalar connMgr sql
let execReader sql = Sql.execReader connMgr sql
let execReaderf sql = Sql.execReaderF connMgr sql
let execNonQueryf sql = Sql.execNonQueryF connMgr sql
let exec a = Sql.execNonQuery connMgr a [] |> ignore

// create the schema
exec "drop table if exists user"
exec "create table user (id int primary key not null, name varchar not null, address varchar null)"

// a function that inserts a record
let insertUser connMgr (id: int) (name: string) (address: string option) = 
    Sql.execNonQuery connMgr 
        "insert into user (id,name,address) values (@id,@name,@address)"
        (Sql.parameters ["@id", box id; "@name", box name; "@address", box address])

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
let insertNUsers2 n = insertNUsers n |> inTransaction

// executes the transaction, inserting 50 records
insertNUsers2 50 ()

let countUsers(): int64 =
    execScalar "select count(*) from user" [] |> Option.get

printfn "%d users" (countUsers())

let printUser (dr: IDataRecord) =
    let id = (dr?id).Value
    let name = (dr?name).Value
    let address = 
        match dr?address with
        | None -> "No registered address"
        | Some x -> x
    printfn "Id: %d; Name: %s; Address: %s" id name address

execReader "select * from user" []
|> Seq.ofDataReader
|> Seq.iter printUser

execNonQueryf "delete from user where id > %d" 10 |> ignore

printfn "Now there are %d users" (countUsers())

// a record type representing a row in the table
type User = {
    id: int
    name: string
    address: string option
}

// maps a raw data record as a User record
let asUser (r: #IDataRecord) =
    {id = (r?id).Value; name = (r?name).Value; address = r?address}

// get the first user
let firstUser = execReader "select * from user limit 1" [] |> Sql.mapOne asUser 

printfn "first user's name: %s" firstUser.name
printfn "first user does%s have an address" (if firstUser.address.IsNone then " not" else "")