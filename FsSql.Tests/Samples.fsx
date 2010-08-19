#r "bin\debug\FsSql.dll"
#r "bin\debug\System.Data.SQlite.dll"

open System.Data

let openConn() =
    let conn = new System.Data.SQLite.SQLiteConnection("Data Source=test.db;Version=3;New=True;")
    conn.Open()
    conn :> IDbConnection

let connMgr = Sql.withNewConnection openConn
let exec a = Sql.execNonQuery connMgr a [] |> ignore
exec "drop table if exists user"
exec "create table user (id int primary key not null, name varchar not null, address varchar null)"

let insertUser connMgr id name address = 
    Sql.execNonQuery connMgr 
        "insert into user (id,name,address) values (@id,@name,@address)"
        (Sql.parameters ["@id", box id; "@name", box name; "@address", Sql.writeOption address])

let insertNUsers n conn =
    let insertUser = insertUser conn
    for i in 1..n do
        let name = sprintf "pepe %d" i
        let address = 
            if i % 2 = 0
                then None
                else Some (sprintf "fake st %d" i)
        insertUser i name address |> ignore

let insertNUsers2 n = insertNUsers n |> Sql.transactional connMgr

insertNUsers2 50 ()

let printUser (dr: IDataRecord) =
    let id = (dr?id).Value
    let name = (dr?name).Value
    let address = 
        match dr?address with
        | None -> "No registered address"
        | Some x -> x
    printfn "Id: %d; Name: %s; Address: %s" id name address

Sql.execReader connMgr "select * from user" []
|> Seq.ofDataReader
|> Seq.iter printUser
