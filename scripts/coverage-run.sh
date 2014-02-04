#!/bin/bash

JAR=~/NetBeansProjects/WikiMining/target/WikiMining-1.0-SNAPSHOT.jar
DIR=../smallest
#DIR=../20121001
PARTITIONS=10
SELECT=4

ORIGINAL_PATH="$PATH"
HADOOP_PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin"
HADOOP_ONE_PATH="$PATH:/home/uvictor/hadoop-1.2.1/bin"
HADOOP_TWO_PATH="$PATH:/home/uvictor/hadoop-2.2.0/bin"
MAHOUT_PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin:/home/uvictor/mahout/bin"

echoerr() {
  echo "$@" 1>&2;
}

export PATH=$HADOOP_PATH
time ./first-word-coverage.sh $JAR \
	ch.ethz.las.wikimining.mr.coverage.h104.GreeDiFirst \
	$DIR \
	$SELECT \
	$PARTITIONS
export PATH=$ORIGINAL_PATH
echo -e "Coverage - GreeDi First Pass\t\tDONE"

export PATH=$HADOOP_TWO_PATH
time ./second-word-coverage.sh $JAR \
	ch.ethz.las.wikimining.mr.coverage.GreeDiSecond \
	$DIR \
	$SELECT
export PATH=$ORIGINAL_PATH
echo -e "Coverage - GreeDi Second Pass\t\tDONE"

