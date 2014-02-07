#!/bin/bash

source default.vars

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.GenerateBasisMatrix \
	-output $DIR/influence-basis-matrix \
	-dimensions $DIMENSIONS
echo -e "Influence - Generate Basis Matrix\t\tDONE"
export PATH=$ORIGINAL_PATH

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.DocumentDate \
	-input $DIR/$XML \
	-output $DIR/date
echo -e "Influence - Document Date\t\tDONE"
export PATH=$ORIGINAL_PATH

