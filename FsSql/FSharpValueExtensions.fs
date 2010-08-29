namespace Microsoft.FSharp.Reflection

open System
open System.Reflection
open Microsoft.FSharp.Reflection

[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module FSharpValue =
    let MakeOptionNone (t: Type) = 
        let opt = FSharpType.MakeOptionType t
        opt.InvokeMember("None", BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.GetProperty, null, null, null)

    let MakeOptionSome (t: Type) (value: obj) =
        let opt = FSharpType.MakeOptionType t
        opt.InvokeMember("Some", BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.InvokeMethod, null, null, [| value |])
        
    let GetOptionValue (opt: obj) =
        if opt = null 
            then nullArg "opt"
        let t = opt.GetType()
        if not (FSharpType.IsOption t)
            then invalidArg "opt" "Object must be of option type"
        unbox <| t.InvokeMember("Value", BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.GetProperty, null, opt, null)

    let IsOption (opt: obj) =
        if opt = null
            then false
            else FSharpType.IsOption (opt.GetType())

    let IsNone (opt: obj): bool =
        if not (IsOption opt)
            then invalidArg "opt" "Object must be of option type"
            else unbox <| opt.GetType().GetMethod("get_IsNone").Invoke(null, [| opt |])

    let IsSome (opt: obj): bool =
        if not (IsOption opt)
            then invalidArg "opt" "Object must be of option type"
            else unbox <| opt.GetType().GetMethod("get_IsSome").Invoke(null, [| opt |])

    let (|OptionType|NotOptionType|) x = 
        if IsOption x
            then OptionType
            else NotOptionType

    let (|OSome|_|) (x: obj) =
        if not (IsOption x)
            then invalidArg "a" "Object must be of option type"
            else 
                if IsNone x
                    then None
                    else Some (GetOptionValue x)
