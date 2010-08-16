module Seq

open System
open System.Data
open FsSqlPrelude
open FsSqlImpl

let ofDataReader (dr: IDataReader) =
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
            dr.Dispose()}
