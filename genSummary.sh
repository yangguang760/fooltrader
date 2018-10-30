#!/bin/bash -e

BASEDIR=`dirname $0`

source $BASEDIR/ve/bin/activate
cd $BASEDIR
export PYTHONPATH=$PYTHONPATH:.

exec python $BASEDIR/genSummary.py
