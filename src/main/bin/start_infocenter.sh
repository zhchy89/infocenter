#!/bin/bash 
# Absolute path this script is in, thus /home/user/bin

SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$SCRIPTPATH/..
PERFORMANCE_TESTING=$1

SERVICE_RUNNING_PATH=$(dirname "$SCRIPTPATH")

CONFIGFILE=$ROOTPATH/config/jvm.properties
METRIC_CONFIGFILE=$ROOTPATH/config/metric.properties

findStr()
{
    local target=$1
    local file=$2
    #echo target : $target
    #echo file : $file
    sed '/^\#/d' ${file} | grep ${target} | sed -e 's/ //g' |
        while read LINE
        do
            local KEY=`echo $LINE | cut -d "=" -f 1`
            local VALUE=`echo $LINE | cut -d "=" -f 2`
            [ ${KEY} = ${target} ] && {
                local UNKNOWN_NAME=`echo $VALUE | grep '\${.*}' -o | sed 's/\${//' | sed 's/}//'`
                if [ $UNKNOWN_NAME ];then
                    local UNKNOWN_VALUE=`findStr ${UNKNOWN_NAME} ${file}`
                    echo ${VALUE} | sed s/\$\{${UNKNOWN_NAME}\}/${UNKNOWN_VALUE}/
                else
                    echo $VALUE
                fi
                return 
            }
        done
    return
}

xms=$( findStr initial.mem.pool.size $CONFIGFILE )
xmx=$( findStr max.mem.pool.size $CONFIGFILE )
max_gc_pause_ms=$( findStr max.gc.pause.ms $CONFIGFILE )
gc_pause_interval_ms=$( findStr gc.pause.internal.ms $CONFIGFILE )
parallel_gc_threads=$( findStr parallel.gc.threads $CONFIGFILE )

metric_enable=$( findStr metric.enable $METRIC_CONFIGFILE )
metric_enable_profiles=$( findStr metric.enable.profiles $METRIC_CONFIGFILE )
metric_port=$( findStr metric.remote.port $METRIC_CONFIGFILE )

metric_params="-Dmetric.enable=$metric_enable -Dmetric.enable.profiles=$metric_enable_profiles -Dcom.sun.management.jmxremote.port=$metric_port -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false"

java -noverify  $metric_params -server -Xms$xms -Xmx$xmx -XX:+UseG1GC -XX:MaxGCPauseMillis=$max_gc_pause_ms -XX:GCPauseIntervalMillis=$gc_pause_interval_ms -XX:ParallelGCThreads=$parallel_gc_threads -cp "$ROOTPATH:$ROOTPATH/lib/*:$ROOTPATH/config" py.infocenter.Launcher  $SERVICE_RUNNING_PATH 2>&1 > /dev/null
