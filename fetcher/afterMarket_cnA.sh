#!/bin/bash
DIR=$(dirname `readlink -f $0`)
cd $DIR
ps aux|grep 'python.*TuShareAfterMarket.py'|grep -v grep|awk '{print $2;}'|xargs kill -9
python ./TuShareAfterMarket.py 2>&1 >> /tmp/TuShareAfterMarket.log &

