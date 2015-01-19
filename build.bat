@echo off
@powershell -NoProfile -Command "(new-object net.webclient).DownloadFile('http://nuget.org/nuget.exe', 'NuGet.exe')"
NuGet.exe restore
msbuild FsSql.sln