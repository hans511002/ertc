#!/usr/bin/env bash

usage="Usage: collect-cleanup.sh (--cleanZk)"

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# This will set ESTORM_HOME, etc.
. "$bin"/collect-config.sh

case $1 in
  --cleanZk)
    matches="yes" ;;
  *) ;;
esac
if [ $# -ne 1 -o "$matches" = "" ]; then
  echo $usage
  exit 1;
fi

format_option=$1;

$bin/collect clean --cleanZk  > /dev/null 2>&1
