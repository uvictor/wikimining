#!/bin/bash

source vars.sh

export PATH=$HADOOP_TWO_PATH
hadoop fs \
	-libjars $LIBJARS \
	-text $1
export PATH=$ORIGINAL_PATH
