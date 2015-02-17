module List

open System.Data

/// Generates a new list from the given datareader.
let ofDataReader (dr: #IDataReader) =
    dr |> Seq.ofDataReader |> List.ofSeq
