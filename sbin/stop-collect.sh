#!/usr/bin/env bash
#
# Modelled after $COLLECT_HOME/sbin/stop-collect.sh.

# Stop storm collect daemons.  Run this on master node.

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/collect-config.sh
. "$bin"/collect-common.sh

# variables needed for stop command
if [ "$COLLECT_LOG_DIR" = "" ]; then
  export COLLECT_LOG_DIR="$COLLECT_HOME/logs"
fi
mkdir -p "$COLLECT_LOG_DIR"

if [ "$COLLECT_IDENT_STRING" = "" ]; then
  export COLLECT_IDENT_STRING="$USER"
fi

export COLLECT_LOG_PREFIX=hbase-$COLLECT_IDENT_STRING-master-$HOSTNAME
export COLLECT_LOGFILE=$COLLECT_LOG_PREFIX.log
logout=$COLLECT_LOG_DIR/$COLLECT_LOG_PREFIX.out
loglog="${COLLECT_LOG_DIR}/${COLLECT_LOGFILE}"
pid=${COLLECT_PID_DIR:-/tmp}/collect-$COLLECT_IDENT_STRING-master.pid

echo -n "stopping COLLECT   "  
 
"$bin"/collect-daemons.sh --config "${COLLECT_CONF_DIR}" --hosts "${COLLECT_MASTERS}" stop master

 
