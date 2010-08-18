namespace FsSqlImpl

open System.Data
open FsSqlPrelude

type DataReaderWrapper(dr: IDataReader, dispose: unit -> unit) =
    interface IDataReader with
        member x.Close() = dr.Close()
        member x.Dispose() = 
            log "DataReaderWrapper dispose"
            if not (dr.IsClosed)
                then dr.Dispose()
            dispose()
        member x.GetBoolean i = dr.GetBoolean i
        member x.GetByte i = dr.GetByte i
        member x.GetBytes(i, fieldOffset, buffer, bufferoffset, lenght) = dr.GetBytes(i, fieldOffset, buffer, bufferoffset, lenght)
        member x.GetChar i = dr.GetChar i
        member x.GetChars(i, fieldOffset, buffer, bufferoffset, lenght) = dr.GetChars(i, fieldOffset, buffer, bufferoffset, lenght)
        member x.GetData i = dr.GetData i
        member x.GetDataTypeName i = dr.GetDataTypeName i
        member x.GetDateTime i = dr.GetDateTime i
        member x.GetDecimal i = dr.GetDecimal i
        member x.GetDouble i = dr.GetDouble i
        member x.GetFieldType i = dr.GetFieldType i
        member x.GetFloat i = dr.GetFloat i
        member x.GetGuid i = dr.GetGuid i
        member x.GetInt16 i = dr.GetInt16 i
        member x.GetInt32 i = dr.GetInt32 i
        member x.GetInt64 i = dr.GetInt64 i
        member x.GetName i = dr.GetName i
        member x.GetOrdinal name = dr.GetOrdinal name
        member x.GetSchemaTable() = dr.GetSchemaTable()
        member x.GetString i = dr.GetString i
        member x.GetValue i = dr.GetValue i
        member x.GetValues values = dr.GetValues values
        member x.IsDBNull i = dr.IsDBNull i
        member x.NextResult() = dr.NextResult()
        member x.Read() = dr.Read()
        member x.FieldCount with get() = dr.FieldCount
        member x.Depth with get() = dr.Depth
        member x.IsClosed with get() = dr.IsClosed
        member x.RecordsAffected with get() = dr.RecordsAffected
        member x.Item 
            with get (name: string) : obj = dr.[name]
        member x.Item 
            with get (i: int) : obj = dr.[i]
