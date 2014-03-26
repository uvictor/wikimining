#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.GreeDiFirst \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-input $DIR/influence-novelty \
	-output $DIR/influence-subset \
	-dates $DIR/date \
	-spread $DIR/influence-wordspread \
	-partitions $PARTITIONS \
	-select $SELECT
echo -e "Influence - GreeDi First Pass\t\tDONE"
export PATH=$ORIGINAL_PATH

