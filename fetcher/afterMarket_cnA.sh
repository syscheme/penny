#!/bin/bash
DIR=$(dirname `readlink -f $0`)
cd $DIR
python ./TuShareAfterMarket.py 2>&1 > TuShareAfterMarket.log

