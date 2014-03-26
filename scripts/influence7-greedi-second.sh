#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.GreeDiSecond \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-input $DIR/influence-novelty \
	-output $DIR/influence-selected \
	-dates $DIR/date \
	-spread $DIR/influence-wordspread \
	-docs $DIR/influence-subset \
	-select $SELECT
echo -e "Influence - GreeDi Second Pass\t\tDONE"
export PATH=$ORIGINAL_PATH

