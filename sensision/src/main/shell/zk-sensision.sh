#!/bin/sh

ZOOKEEPER_PORT=2181
ZOOKEEPER_CELL=pocovh
LABELS=cell=${ZOOKEEPER_CELL}

ts=`date +%s`000000
for command in 'mntr'
do
#  netstat -an|grep LISTEN|grep ${ZOOKEEPER_PORT}|sed -e 's/:2181 .*//' -e 's/.* //'|while read ip;
  ifconfig -a|grep inet|grep -v 'inet6'|sed -e 's/^.*inet\s\(addr:\)\?//'|awk '{ print $1 }'|while read ip;
  do
    echo "${command}"|nc $ip ${ZOOKEEPER_PORT};
  done | while read k v
  do
    if [ "${command}.${k}" == "mntr.zk_version" ]
    then
      continue
    fi
    if [ "${command}.${k}" == "mntr.zk_server_state" ]
    then
      echo "${ts}// zookeeper.${command}.${k}{${LABELS}} '${v}'"
      continue
    fi
    echo "${ts}// zookeeper.${command}.${k}{${LABELS}} ${v}"
  done
done

