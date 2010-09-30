namespace Microsoft.FSharp.Reflection

open System
open System.Reflection
open Microsoft.FSharp.Reflection

[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module FSharpValue =
    /// <summary>
    /// Creates a None option for type <paramref name="t"/>
    /// </summary>
    let MakeOptionNone (t: Type) = 
        let opt = FSharpType.MakeOptionType t
        opt.InvokeMember("None", BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.GetProperty, null, null, null)

    /// <summary>
    /// Creates a Some option for type <paramref name="t"/> and value <paramref name="value"/>
    /// </summary>
    let MakeOptionSome (t: Type) (value: obj) =
        let opt = FSharpType.MakeOptionType t
        opt.InvokeMember("Some", BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.InvokeMethod, null, null, [| value |])

    /// <summary>
    /// Gets the value associated with an option
    /// </summary>
    /// <exception cref="System.NullReferenceException"><paramref value="opt"/> is null</exception>
    /// <exception cref="System.ArgumentException"><paramref value="opt"/> is not an option</exception>
    let GetOptionValue (opt: obj) =
        if opt = null 
            then nullArg "opt"
        let t = opt.GetType()
        if not (FSharpType.IsOption t)
            then invalidArg "opt" "Object must be of option type"
        unbox <| t.InvokeMember("Value", BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.GetProperty, null, opt, null)

    /// Returns true if object is of option type
    let IsOption (opt: obj) =
        if opt = null
            then false
            else FSharpType.IsOption (opt.GetType())

    /// <summary>
    /// Returns true if object is None
    /// </summary>
    /// <exception cref="System.ArgumentException">Argument is not an option</exception>
    let IsNone (opt: obj): bool =
        if not (IsOption opt)
            then invalidArg "opt" "Object must be of option type"
            else unbox <| opt.GetType().GetMethod("get_IsNone").Invoke(null, [| opt |])

    /// <summary>
    /// Returns true if object is Some x
    /// </summary>
    /// <exception cref="System.ArgumentException">Argument is not an option</exception>
    let IsSome (opt: obj): bool =
        if not (IsOption opt)
            then invalidArg "opt" "Object must be of option type"
            else unbox <| opt.GetType().GetMethod("get_IsSome").Invoke(null, [| opt |])

    /// <summary>
    /// OptionType if value to match is a boxed Option, otherwise NotOptionType
    /// </summary>
    /// <exception cref="System.ArgumentException">Argument is not an option</exception>
    let (|OptionType|NotOptionType|) x = 
        if IsOption x
            then OptionType
            else NotOptionType

    /// <summary>
    /// Extracts value associated with a boxed Option.
    /// </summary>
    /// <exception cref="System.ArgumentException">Argument is not an option</exception>
    let (|OSome|ONone|) (x: obj) : Choice<obj, unit> =
        if IsNone x
            then ONone
            else OSome (GetOptionValue x)
