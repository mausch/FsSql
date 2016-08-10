[<AutoOpen>]
module FsSql.Operators

open System.Data

let (?) (record: #IDataRecord) (field: string): 'a option =
    Sql.readField field record