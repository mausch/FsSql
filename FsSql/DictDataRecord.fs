namespace FsSqls

open System
open System.Data

type DictDataRecord(dr: IDataRecord) =
    interface IDataRecord with
        member x.GetBoolean i = true
        member x.GetByte i = byte 0
        member x.GetBytes(i, fieldOffset, buffer, bufferOffset, length) = 0L
        member x.GetChar i = char 0
        member x.GetChars(i, fieldOffset, buffer, bufferOffset, length) = 0L
        member x.GetData i = null
        member x.GetDataTypeName i = null
        member x.GetDateTime i = DateTime.Now
        member x.GetDecimal i = 0m
        member x.GetDouble i = float 0
        member x.GetFieldType i = null
        member x.GetFloat i = 0.0f
        member x.GetGuid i = Guid.NewGuid()
        member x.GetInt16 i = 0s
        member x.GetInt32 i = i
        member x.GetInt64 i = 0L
        member x.GetName i = ""
        member x.GetOrdinal name = 0
        member x.GetString i = ""
        member x.GetValue i = null
        member x.GetValues values = 0
        member x.IsDBNull i = true
        member x.FieldCount with get() = 0
        member x.Item 
            with get (name: string) : obj = null
        member x.Item 
            with get (i: int) : obj = null
