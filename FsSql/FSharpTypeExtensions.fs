namespace Microsoft.FSharp.Reflection

open System
open System.Reflection
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Core

[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module FSharpType =
    /// Returns true if type is an Option type
    let IsOption (t: Type) = 
        if t.IsGenericType
            then t.GetGenericTypeDefinition() = typedefof<option<_>>
            else false

    /// Returns true if type is a list
    let IsList (t: Type) =
        if t.IsGenericType
            then t.GetGenericTypeDefinition() = typedefof<list<_>>
            else false
        
    /// Creates an option type
    let MakeOptionType (t: Type) =
        typedefof<option<_>>.MakeGenericType [| t |]

    let tryGetMethod (name: string) (types: Type[]) (t: Type) = 
        let m = t.GetMethod(name, types)
        if m = null
            then None
            else Some m