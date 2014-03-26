#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.TfIdfNovelty \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-ignore \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/influence-dup-novelty \
	-basis $DIR/influence-basis-matrix \
	-rows $ROWS \
	-bands $BANDS \
	-dates $DIR/date
echo -e "Influence - TfIdf Novelty (LSH)\t\tDONE"
export PATH=$ORIGINAL_PATH

