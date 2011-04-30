module Option

open System

/// Maps DBNull to None, otherwise Some x
let fromDBNull (o: obj): 'a option =
    try 
        if o = null || DBNull.Value.Equals o
            then None
            else Some (unbox o)
    with :? InvalidCastException as e ->
        let msg = sprintf "Can't cast '%s' to '%s'" (o.GetType().Name) (typeof<'a>.Name)
        raise <| InvalidCastException(msg, e)

/// Maps None to DBNull, otherwise the option's value
let toDBNull x =
    defaultArg x (box DBNull.Value)

/// Maps None to a default value, otherwise the option's value
let getOrDefault x =
    defaultArg x Unchecked.defaultof<'a>

let fromBool b = 
    if b then Some() else None