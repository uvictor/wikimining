#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.coverage.h104.RevisionsConvertor \
  $DIR/revisions_all_stats.txt \
  $DIR/revisions \
  both
export PATH=$ORIGINAL_PATH
echo -e "Revisions - Count and Volume Convertor\t\tDONE"
