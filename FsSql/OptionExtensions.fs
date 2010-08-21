module Option

open System

/// Maps DBNull to None, otherwise Some x
let fromDBNull (o: obj) =
    if DBNull.Value.Equals o
        then None
        else Some (unbox o)

/// Maps None to DBNull, otherwise the option's value
let toDBNull =
    function
    | None -> box DBNull.Value
    | Some x -> box x

/// Maps None to a default value, otherwise the option's value
let getOrDefault =
    function
    | None -> Unchecked.defaultof<'a>
    | Some x -> x
