@echo off
@powershell -NoProfile -Command "if (!(test-path 'NuGet.exe')) { (new-object net.webclient).DownloadFile('http://nuget.org/nuget.exe', 'NuGet.exe') }"
NuGet.exe restore
msbuild /m /p:Configuration=Release /p:TargetFrameworkVersion=v2.0 /t:Rebuild FsSql.sln
msbuild /m /p:Configuration=Release /p:TargetFrameworkVersion=v4.0 /t:Rebuild FsSql.sln
NuGet.exe pack