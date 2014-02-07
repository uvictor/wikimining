#!/bin/bash

source default.vars

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

