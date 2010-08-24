module Seq

open System
open System.Data
open FsSqlPrelude
open FsSqlImpl

/// Generates a new forward-only sequence from the given datareader.
let ofDataReader (dr: #IDataReader) =
    log "started ofDataReader"
    let lockObj = obj()
    let lockReader f = lock lockObj f
    let read()() =            
        let h = dr.Read()
        //logf "read record %A" dr.["id"]
        if h
            then DictDataRecord(dr) :> IDataRecord
            else null
    let lockedRead = lockReader read
    let records = Seq.initInfinite (fun _ -> lockedRead())
                    |> Seq.takeWhile (fun r -> r <> null)
    seq {
        try
            yield! records
        finally
            log "datareader dispose"
            if not (dr.IsClosed)
                then dr.Dispose()}

let mapSnd f sequence = 
    sequence |> Seq.map (fun (a,b) -> a,f b)

let mapSndMap f sequence =
    sequence |> mapSnd (Seq.map f)

let groupByFst sequence =
    sequence 
    |> Seq.groupBy fst
    |> mapSndMap snd
    
let groupByFstSnd sequence =
    sequence 
    |> Seq.groupBy (fun (a,b,_) -> a,b)
    |> mapSndMap (fun (_,_,x) -> x)

let groupBy123 sequence =
    sequence
    |> Seq.groupBy (fun (a,b,c,_) -> a,b,c)
    |> mapSndMap (fun (_,_,_,x) -> x)