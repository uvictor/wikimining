#!/bin/bash

source vars.sh

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.GenerateBasisMatrix \
	-output $DIR/influence-basis-matrix \
	-rows $ROWS \
	-bands $BANDS \
	-dimensions $DIMENSIONS
echo -e "Influence - Generate Basis Matrix\t\tDONE"
export PATH=$ORIGINAL_PATH

