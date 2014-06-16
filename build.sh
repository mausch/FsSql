NUGET=./.nuget/NuGet.exe
if [ ! -f $NUGET ]; then
  wget http://nuget.org/nuget.exe
  mv nuget.exe $NUGET 
  chmod a+x $NUGET
fi
xbuild
