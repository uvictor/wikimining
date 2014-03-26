#!/bin/bash

source vars.sh

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.coverage.h104.GreeDiSecond \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-input $DIR/tfidf/tfidf-vectors \
	-docs $DIR/graph-subset \
	-output $DIR/graph-selected \
  -wordCount $DIR/tfidf/df-count \
  -wordCountType df \
  -graph $DIR/graph \
	-select $SELECT
export PATH=$ORIGINAL_PATH
echo -e "Graph - GreeDi Second Pass\t\tDONE"

