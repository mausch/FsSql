#!/usr/bin/env bash

set -eu
set -o pipefail

cd `dirname $0`
OS=${OS:-"unknown"}

function run() {
  if [[ "$OS" != "Windows_NT" ]]
  then
    mono "$@"
  else
    "$@"
  fi
}

[[ -f tools/paket.exe ]] || run tools/paket.bootstrapper.exe

if [[ "$OS" != "Windows_NT" ]] && [ ! -e ~/.config/.mono/certs ]
then
  mozroots --import --sync --quiet
fi

echo 'Restoring nugets'
run tools/paket.exe restore

echo 'Ensure Logging namespace'
ruby -pi.bak -e \
  "gsub(/namespace Logary.Facade/, 'namespace FsSql.Logging')" \
  paket-files/logary/logary/src/Logary.Facade/Facade.fs


echo 'Building'
xbuild FsSql.sln /p:Configuration=Release

echo 'Running tests'
mono FsSql.Tests/bin/Release/FsSql.Tests.exe
