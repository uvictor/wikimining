#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.coverage.h104.GreeDiFirst \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/revisions-subset \
  -wordCount $DIR/tfidf/df-count \
  -wordCountType df \
  -graph $MASTER_DIR/graph \
  -revisions $MASTER_DIR/revisions \
	-select $SELECT \
	-partitions $PARTITIONS
export PATH=$ORIGINAL_PATH
echo -e "Revisions++ - GreeDi First Pass\t\tDONE"

