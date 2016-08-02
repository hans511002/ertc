#!/usr/bin/env bash

# Runs a collect command as a daemon.
#
# Environment Variables
#
#   COLLECT_CONF_DIR   Alternate collect conf dir. Default is ${COLLECT_HOME}/conf.
#   COLLECT_LOG_DIR    Where log files are stored.  PWD by default.
#   COLLECT_PID_DIR    The pid files are stored. /tmp by default.
#   COLLECT_IDENT_STRING   A string representing this instance of hadoop. $USER by default
#   COLLECT_NICENESS The scheduling priority for daemons. Defaults to 0.
#   COLLECT_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server if it has not stopped.
#                        Default 1200 seconds.
#
# Modelled after $HADOOP_HOME/sbin/collect-daemon.sh

usage="Usage: collect-daemon.sh [--config <conf-dir>]\
 (start|stop|restart|autorestart) <collect-command> \
 <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/collect-config.sh
. "$bin"/collect-common.sh

# get arguments
startStop=$1
shift

command=$1
shift

collect_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

cleanZNode() {
  
        $bin/collect clean --cleanZk  > /dev/null 2>&1
    
}

check_before_start(){
    #ckeck if the process is not running
    mkdir -p "$COLLECT_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
}

wait_until_done ()
{
    p=$1
    cnt=${COLLECT_SLAVE_TIMEOUT:-300}
    origcnt=$cnt
    while kill -0 $p > /dev/null 2>&1; do
      if [ $cnt -gt 1 ]; then
        cnt=`expr $cnt - 1`
        sleep 1
      else
        echo "Process did not complete after $origcnt seconds, killing."
        kill -9 $p
        exit 1
      fi
    done
    return 0
}

# get log directory
if [ "$COLLECT_LOG_DIR" = "" ]; then
  export COLLECT_LOG_DIR="$COLLECT_HOME/logs"
fi
mkdir -p "$COLLECT_LOG_DIR"

if [ "$COLLECT_PID_DIR" = "" ]; then
  COLLECT_PID_DIR=$COLLECT_LOG_DIR
fi

if [ "$COLLECT_IDENT_STRING" = "" ]; then
  export COLLECT_IDENT_STRING="$USER"
fi

# Some variables
# Work out java location so can print version into log.
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
export COLLECT_LOG_PREFIX=collect-$COLLECT_IDENT_STRING-$command-$HOSTNAME
export COLLECT_LOGFILE=$COLLECT_LOG_PREFIX.log
export COLLECT_ROOT_LOGGER=${COLLECT_ROOT_LOGGER:-"INFO,RFA"}
export COLLECT_SECURITY_LOGGER=${COLLECT_SECURITY_LOGGER:-"INFO,RFAS"}
logout=$COLLECT_LOG_DIR/$COLLECT_LOG_PREFIX.out
loggc=$COLLECT_LOG_DIR/$COLLECT_LOG_PREFIX.gc
loglog="${COLLECT_LOG_DIR}/${COLLECT_LOGFILE}"
pid=$COLLECT_PID_DIR/collect-$COLLECT_IDENT_STRING-$command.pid
 export COLLECT_START_FILE=$COLLECT_PID_DIR/collect-$COLLECT_IDENT_STRING-$command.autorestart

if [ -n "$SERVER_GC_OPTS" ]; then
  export SERVER_GC_OPTS=${SERVER_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${loggc}"}
fi

# Set default scheduling priority
if [ "$COLLECT_NICENESS" = "" ]; then
    export COLLECT_NICENESS=0
fi

thiscmd=$0
args=$@

case $startStop in

(start)
    check_before_start
    collect_rotate_log $logout
    collect_rotate_log $loggc
    echo starting $command, logging to $logout
    nohup $thiscmd --config "${COLLECT_CONF_DIR}" internal_start $command $args < /dev/null > ${logout} 2>&1  &
    sleep 1; head "${logout}"
  ;;

(internal_start)
    # Add to the command log file vital stats on our environment.
    echo "`date` Starting $command on `hostname`" >> $loglog
    echo "`ulimit -a`" >> $loglog 2>&1
    nice -n $COLLECT_NICENESS "$COLLECT_HOME"/sbin/collect \
        --config "${COLLECT_CONF_DIR}" \
        $command "$@" start >> "$logout" 2>&1 &
    echo $! > $pid
    wait
	echo `ps ax | grep -i 'CollectMain' | grep java | grep -v grep | awk '{print $1}' ` > $pid
  ;;
 
(stop)
    rm -f "$COLLECT_START_FILE"
    if [ -f $pid ]; then
      pidToKill=`cat $pid`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $loglog
        kill -2 $pidToKill > /dev/null 2>&1
        "$COLLECT_HOME"/sbin/collect master stop
        rm -rf $pid
      else
        retval=$?
        echo no $command to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $pid
    fi
  ;;

(restart)
    # stop the command
    $thiscmd --config "${COLLECT_CONF_DIR}" stop $command $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${COLLECT_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi
    # start the command
    $thiscmd --config "${COLLECT_CONF_DIR}" start $command $args &
    wait_until_done $!
  ;;

(*)
  echo $usage
  exit 1
  ;;
esac
