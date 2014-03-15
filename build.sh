#!/bin/bash
if [ ! -f packages/FAKE/tools/FAKE.exe ]; then
  mono .nuget/NuGet.exe install FAKE -OutputDirectory packages -ExcludeVersion
fi
export FSHARPI=`which fsharpi`
PATH="$PWD:$PATH"
mono packages/FAKE/tools/FAKE.exe build.fsx $@
