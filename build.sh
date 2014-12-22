NUGET=./.nuget/NuGet.exe
if [ ! -f $NUGET ]; then
  curl -L -o $NUGET https://www.nuget.org/nuget.exe
  chmod a+x $NUGET
fi
mono .nuget/NuGet.exe restore
xbuild
