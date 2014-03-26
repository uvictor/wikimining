#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.coverage.h104.GreeDiFirst \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/coverage-lsh-subset \
  -buckets $DIR/influence-lsh-buckets \
	-select $SELECT \
	-partitions $PARTITIONS
export PATH=$ORIGINAL_PATH
echo -e "Coverage Lsh - GreeDi First Pass\t\tDONE"

