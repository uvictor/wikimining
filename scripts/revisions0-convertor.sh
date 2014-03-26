#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.coverage.h104.RevisionsConvertor \
  $DIR/revisions_all_stats.txt \
  $DIR/revisions
export PATH=$ORIGINAL_PATH
echo -e "Revisions - Count Convertor\t\tDONE"
