#!/bin/bash

base_dir=`dirname $0`
source ${base_dir}/install-env.sh

if [ ! -d "$APP" ]; then
    mkdir -p "$APP"
fi

unzip $INSTALL_KFKCONS -d $APP

echo "install $KFKCONS finished, path : $KFKCONS_HOME"
