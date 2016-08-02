#!/usr/bin/env bash

# Set environment variables here.

# This script sets variables multiple times over the course of starting an hbase process,
# so try to keep things idempotent unless you want to take an even deeper look
# into the startup scripts (bin/hbase, etc.)

# The java implementation to use.  Java 1.6 required.
# export JAVA_HOME=/usr/java/jdk1.6.0/
. ~/.bash_profile

# Extra Java CLASSPATH elements.  Optional.
bin=`dirname $0`
bin=`cd "$bin"; pwd`

if [ "x$COLLECT_HOME" == "x" ] ; then
   export COLLECT_HOME=`cd "$bin/../"; pwd`
fi

export COLLECT_CLASSPATH=$COLLECT_HOME/conf

#for f in $COLLECT_HOME/lib/*.jar ; do
#    COLLECT_CLASSPATH=${COLLECT_CLASSPATH}:$f;
#done
export COLLECT_CLASSPATH
if [ "$COLLECT_PID_DIR" == "" ] ; then
   export COLLECT_PID_DIR=$COLLECT_HOME/logs
fi
if [ "$COLLECT_LOG_DIR" == "" ] ; then
   export COLLECT_PID_DIR=$COLLECT_HOME/logs
fi

export DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8000"

