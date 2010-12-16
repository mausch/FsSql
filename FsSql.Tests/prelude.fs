[<AutoOpen>]
module TestPrelude

open MbUnit.Framework

let assertThrows<'e when 'e :> exn> f =
    let action = Gallio.Common.Action f
    Assert.Throws<'e> action |> ignore

let catch defaultValue f a =
    try
        f a
    with e -> defaultValue

let failwithe (e: #exn) msg = raise <| System.Exception(msg, e)
let failwithef (e: #exn) = Printf.kprintf (failwithe e)
