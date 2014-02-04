#!/bin/bash

ORIGINAL_PATH="$PATH"

# Mahout currently works only with Hadoop 1.x.x
export PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin:/home/uvictor/mahout/bin"

mahout "$@"

export PATH=$ORIGINAL_PATH

