#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.coverage.h104.GreeDiSecond \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/coverage-lsh-selected \
  -buckets $DIR/influence-lsh-buckets \
	-select $SELECT \
	-docs $DIR/coverage-lsh-subset
export PATH=$ORIGINAL_PATH
echo -e "Coverage Lsh - GreeDi Second Pass\t\tDONE"

