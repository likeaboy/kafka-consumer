#!/bin/bash

base_dir=`dirname $0`
source ${base_dir}/install-env.sh

cd $KFKCONS_HOME && sh startup.sh
echo "start $KFKCONS"
