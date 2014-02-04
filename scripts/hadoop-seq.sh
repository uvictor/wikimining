#!/bin/bash

TARGET=~/NetBeansProjects/WikiMining/target
LIBJARS=$TARGET/WikiMining-1.0-SNAPSHOT.jar

ORIGINAL_PATH="$PATH"
HADOOP_PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin"
HADOOP_TWO_PATH="$PATH:/home/uvictor/hadoop-2.2.0/bin"

export PATH=$HADOOP_TWO_PATH
hadoop fs \
	-libjars $LIBJARS \
	-text $1
export PATH=$ORIGINAL_PATH
