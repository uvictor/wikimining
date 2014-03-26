#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.coverage.h104.GraphInlinksCounter \
  -input $DIR/graph \
  -output $DIR/inlinks
export PATH=$ORIGINAL_PATH
echo -e "Graph - Inlinks Counter\t\tDONE"
