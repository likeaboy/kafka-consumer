#!/bin/sh
taskset -c 19,20 nohup java -Xms8096m -Xmx8096m -Xmn4680m -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:gc/gc.log -jar kafka-consumer.jar > /dev/null 2>&1 &