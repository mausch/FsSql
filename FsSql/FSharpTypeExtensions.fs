namespace Microsoft.FSharp.Reflection

open System
open System.Reflection
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Core

[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module FSharpType =
    let IsOption (t: Type) = 
        if t.IsGenericType
            then t.GetGenericTypeDefinition() = typedefof<option<_>>
            else false

    let IsList (t: Type) =
        if t.IsGenericType
            then t.GetGenericTypeDefinition() = typedefof<list<_>>
            else false
        
    let MakeOptionType (t: Type) =
        typedefof<option<_>>.MakeGenericType [| t |]
