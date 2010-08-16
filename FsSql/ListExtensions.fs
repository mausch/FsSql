module List

open System.Data

let ofDataReader (dr: #IDataReader) =
    dr |> Seq.ofDataReader |> List.ofSeq
