#!/bin/bash

source vars.sh

# Mahout currently works only with Hadoop 1
export PATH=$MAHOUT_SRC_PATH
echo $PATH
mahout seqdirectory \
	-i $DIR/raw \
	-o $DIR/plain \
	-c UTF-8 \
	-ow

export PATH=$ORIGINAL_PATH
echo -e "Preprocess - NIPS - Mahout Seq Directory\t\tDONE"
