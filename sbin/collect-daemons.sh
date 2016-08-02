#!/usr/bin/env bash
#
#
# Run a collect command on all master hosts.
# Modelled after $COLLECT_HOME/bin/collect-daemons.sh

usage="Usage: collect-daemons.sh [--config <collect-confdir>] \
 [--hosts serversfile] [start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. $bin/collect-config.sh

remote_cmd="cd ${COLLECT_HOME}; $bin/collect-daemon.sh --config ${COLLECT_CONF_DIR} $@"
args="--hosts ${COLLECT_MASTERS} --config ${COLLECT_CONF_DIR} $remote_cmd"

command=$2
case $command in
  (master|server)
    exec "$bin/servers.sh" $args
    ;;
  (*)
   # exec "$bin/servers.sh" $args
    ;;
esac

