#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.TfIdfNovelty \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-ignore \
  -outputBuckets \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/influence-lsh-buckets \
	-basis $DIR/influence-basis-matrix \
	-rows $ROWS \
	-bands $BANDS
echo -e "Influence - TfIdf Lsh Buckets\t\tDONE"
export PATH=$ORIGINAL_PATH

