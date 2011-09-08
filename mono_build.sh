#!/bin/bash

platform='x86'
monopath='/usr/lib/mono/4.0/'
fscorepath='/usr/local/lib/mono/4.0/'
buildpath='bin/Release/'

`fsc -o:$buildpath'FsSql.dll' --noframework --optimize+ --define:BIGINTEGER --platform:$platform -r:$fscorepath'FSharp.Core.dll' -r:$monopath'mscorlib.dll' -r:$monopath'System.dll' -r:$monopath'System.Data.dll' -r:$monopath'System.Transactions.dll' -r:$monopath'System.Xml.dll' --target:library --warn:4 --warnaserror:76 --vserrors --LCID:1033 --utf8output --fullpaths --flaterrors FsSql/AssemblyInfo.fs FsSql/FSharpTypeExtensions.fs FsSql/FSharpValueExtensions.fs FsSql/prelude.fs FsSql/AsyncExtensions.fs FsSql/OptionExtensions.fs FsSql/DbCommandWrapper.fs FsSql/DictDataRecord.fs FsSql/DataReaderWrapper.fs FsSql/SeqExtensions.fs FsSql/ListExtensions.fs FsSql/FsSql.fs FsSql/Transactions.fs FsSql/FsSqlOperators.fs FsSql/SqlWrapper.fs > /dev/null`
