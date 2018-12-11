#!/bin/bash -e

BASEDIR=`dirname $0`

source /home/yang/pyenvs/fooltrader/ve/bin/activate
cd $BASEDIR
export PYTHONPATH=$PYTHONPATH:.

exec python $BASEDIR/crawlTick.py
