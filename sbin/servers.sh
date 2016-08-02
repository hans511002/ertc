#!/usr/bin/env bash
#
# Run a shell command on all master hosts.
#
# Environment Variables
#
#   COLLECT_MASTERS File naming remote hosts.
#     Default is ${HBASE_CONF_DIR}/masters
#   COLLECT_CONF_DIR  Alternate COLLECT conf dir. Default is ${COLLECT_HOME}/conf.
#   COLLECT_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   COLLECT_SSH_OPTS Options passed to ssh when running remote commands.
#

usage="Usage: $0 [--config <collect-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/collect-config.sh

# If the master backup file is specified in the command line,
# then it takes precedence over the definition in
# kafka-env.sh. Save it here.
HOSTLIST=$COLLECT_MASTERS

if [ "$HOSTLIST" = "" ]; then
  if [ "$COLLECT_MASTERS" = "" ]; then
    export HOSTLIST="${COLLECT_CONF_DIR}/servers"
  else
    export HOSTLIST="${COLLECT_MASTERS}"
  fi
fi

args=${@// /\\ }
args=${args/master-backup/master}

if [ -f $HOSTLIST ]; then
  for emaster in `cat "$HOSTLIST"`; do
   ssh $COLLECT_SSH_OPTS $emaster $"$args  " 2>&1 | sed "s/^/$emaster: /" &
   if [ "$COLLECT_SLAVE_SLEEP" != "" ]; then
     sleep $COLLECT_SLAVE_SLEEP
   fi
  done
fi

wait
