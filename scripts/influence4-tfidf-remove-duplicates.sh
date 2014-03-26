#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.TfIdfRemoveDuplicates \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-input $DIR/influence-dup-novelty \
	-output $DIR/influence-novelty
echo -e "Influence - TfIdf Remove Duplicates\t\tDONE"
export PATH=$ORIGINAL_PATH

