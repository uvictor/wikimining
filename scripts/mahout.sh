#!/bin/bash

source vars.sh

# Mahout currently works only with Hadoop 1.x.x
export PATH=$MAHOUT_PATH
echo $PATH
mahout "$@"
export PATH=$ORIGINAL_PATH

