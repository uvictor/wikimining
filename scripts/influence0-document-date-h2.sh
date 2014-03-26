#!/bin/bash

source vars.sh

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.DocumentDate \
	-input $DIR/$XML \
	-output $DIR/date
echo -e "Influence - Document Date\t\tDONE"
export PATH=$ORIGINAL_PATH

