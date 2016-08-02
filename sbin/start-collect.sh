#!/usr/bin/env bash
#

# Modelled after $COLLECT_HOME/sbin/start-collect.sh.

# Start hadoop hbase daemons.
# Run this on master node.
usage="Usage: start-collect.sh"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/collect-config.sh

# start COLLECT daemons
errCode=$?
if [ $errCode -ne 0 ]
then
  exit $errCode
fi


if [ "$1" = "autorestart" ]
then
  commandToRun="autorestart"
else
  commandToRun="start"
fi

"$bin"/collect-daemons.sh --config "${COLLECT_CONF_DIR}" --hosts "${COLLECT_MASTERS}" $commandToRun master

