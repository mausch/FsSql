module DataTable

open System.Data
open Microsoft.FSharp.Reflection

let ofRecords<'a> (rs : 'a list) = 
    let fields = FSharpType.GetRecordFields typeof<'a>
    let dt = new DataTable()
    // add cols to DataTable
    for f in fields do
        if f.PropertyType.IsPrimitive || f.PropertyType = typeof<string> then
            dt.Columns.Add(f.Name, f.PropertyType) |> ignore
        else failwithf "Only primitive types and strings allowed for table valued params. Field %s was type %A" f.Name f.PropertyType
    // add rows to DataTable
    for r in rs do
        let rowVals = fields |> Array.map (fun f -> f.GetValue(r, null))
        dt.Rows.Add(rowVals) |> ignore
    dt

