namespace FsSqlImpl

open System
open System.Collections.Specialized
open System.Data

type internal Entry = {
    index: int
    name: string
    value: obj
    dataTypeName: string
    fieldType: Type
}

type internal DictDataRecord(dr: IDataRecord) =
    let dic = 
        let x = OrderedDictionary(StringComparer.InvariantCultureIgnoreCase)
        for i in [0..dr.FieldCount-1] do
            let name = dr.GetName i
            let entry = {
                    index = i
                    name = name
                    value = dr.GetValue i
                    dataTypeName = dr.GetDataTypeName i
                    fieldType = dr.GetFieldType i}
            try
                x.Add(name, entry)
            with :? ArgumentException as e -> 
                raise <| ArgumentException("Duplicate field names in resultset", e)
        x

    let getByIndex (i: int) =
        try
            let v = dic.[i]
            Some (v :?> Entry)
        with :? ArgumentOutOfRangeException -> None

    let getByName (key: string) =
        let v = dic.[key]
        if v = null
            then None
            else Some (v :?> Entry)

    let (>>=) x f = Option.bind f x

    let getEntryValue (e: Entry) = e.value |> unbox

    let getValueOrDefault i =
        (getByIndex i >>= (getEntryValue >> Some)) |> Option.getOrDefault |> unbox

    let getValueOrDefaultByName key =
        (getByName key >>= (getEntryValue >> Some)) |> Option.getOrDefault |> unbox

    interface IDataRecord with
        member x.GetBoolean i = getValueOrDefault i
        member x.GetByte i = getValueOrDefault i
        member x.GetBytes(i, fieldOffset, buffer, bufferOffset, length) = raise <| NotImplementedException()
        member x.GetChar i = getValueOrDefault i
        member x.GetChars(i, fieldOffset, buffer, bufferOffset, length) = raise <| NotImplementedException()
        member x.GetData i = raise <| NotSupportedException()
        member x.GetDataTypeName i = (dic.[i] :?> Entry).dataTypeName
        member x.GetDateTime i = getValueOrDefault i
        member x.GetDecimal i = getValueOrDefault i
        member x.GetDouble i = getValueOrDefault i
        member x.GetFieldType i = (dic.[i] :?> Entry).fieldType
        member x.GetFloat i = getValueOrDefault i
        member x.GetGuid i = getValueOrDefault i
        member x.GetInt16 i = getValueOrDefault i
        member x.GetInt32 i = getValueOrDefault i
        member x.GetInt64 i = getValueOrDefault i
        member x.GetName i = (dic.[i] :?> Entry).name
        member x.GetOrdinal name = (dic.[name] :?> Entry).index
        member x.GetString i = getValueOrDefault i
        member x.GetValue i = getValueOrDefault i
        member x.GetValues values = 
            if values = null
                then nullArg "values"
            let count = min values.Length dic.Count
            for i in 0..count-1 do
                values.[i] <- getValueOrDefault i
            count
        member x.IsDBNull i = (getValueOrDefault i) = DBNull.Value
        member x.FieldCount with get() = dic.Count
        member x.Item 
            with get (name: string) : obj = getValueOrDefaultByName name
        member x.Item 
            with get (i: int) : obj = getValueOrDefault i
