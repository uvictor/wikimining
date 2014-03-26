#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.coverage.h104.GraphEdgesConvertor \
  $DIR/page_inlinks.txt \
  $DIR/graph
export PATH=$ORIGINAL_PATH
echo -e "Graph - Edge Convertor\t\tDONE"
