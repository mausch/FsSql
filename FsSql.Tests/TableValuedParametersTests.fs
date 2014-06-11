module FsSql.Tests.TableValuedParametersTests

open Fuchu
open System
open System.Collections.Generic
open System.Data
open System.Data.SqlClient
open FsSqlPrelude
open FsSql.Tests.FsSqlTests

type TableValuedParameterRowType = { value : int }

let getPeopleSproc = @"
CREATE PROCEDURE [dbo].[GetPeople]
	@PersonIds AS dbo.[IntegerList] READONLY,
	@OtherParam AS int
AS
BEGIN
	SET NOCOUNT ON;
	SELECT Id, Name, NULL as Parent from dbo.Person WHERE Id in (SELECT value FROM @PersonIds)
END"

let createConnection () =
    let conn = new SqlConnection("Server=localhost;Database=FsSql.Test;Integrated Security=true")
    conn.Open()
    conn :> IDbConnection

let createSchema conn = 
    let exec a = Sql.execNonQuery conn a [] |> ignore 
    // Person table
    exec "IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetPeople]') AND type in (N'P', N'PC')) DROP PROCEDURE [dbo].[GetPeople]"
    exec "IF EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'IntegerList') DROP TYPE [dbo].[IntegerList]"
    exec "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Person]')) DROP TABLE [dbo].[Person]"
    // People Table
    exec "CREATE TABLE dbo.Person(Id int NOT NULL, Name varchar(50) NOT NULL, CONSTRAINT [PK_Person] PRIMARY KEY CLUSTERED ([Id] ASC))"
    // User defined data type for TVP
    exec "CREATE TYPE [dbo].[IntegerList] AS TABLE([value] [int] NULL)"
    // Stored procedure which accepts the TVP
    exec getPeopleSproc
    log "done creating schema"

let insertPerson conn (p: Person) =
    let idParm = Sql.Parameter.make("@id", p.id)
    let nameParm = Sql.Parameter.make("@name", p.name)
    Sql.execNonQuery conn "insert into person (id, name) values (@id, @name)" [idParm;nameParm] |> ignore

let tvpTests =
    testList "Table Valued Parameters" [
        testCase "table valued parameter together with standard parameter" <|
            fun _ ->
                let conn = Sql.withNewConnection createConnection
                createSchema conn
                for i in 1..3 do 
                    insertPerson conn {id = i; name = (sprintf "Don %i" i); parent = None}
                let personIds = [1;2] |> List.map (fun v -> { value = v })
                ()
                // TODO FIX
//                let tableValuedParam =
//                    TableValued (TableValuedParameter.make("@PersonIds", personIds))
//                let otherParam = 
//                    Standard (Sql.Parameter.make("@OtherParam", 1))
//                let people = 
//                    execSPReader conn "[dbo].[GetPeople]" [tableValuedParam;otherParam]
//                    |> Sql.map (Sql.asRecord<Person> "")
//                    |> Seq.toList
//                Assert.Equal("Should return 2 people", 2, people.Length)
                //    Assert.Equal("First person should be Person 1", 1, people.[
    ]   







