#!/bin/bash

source vars.sh

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.coverage.WikiToPlainText \
	-input $DIR/$XML \
	-output $DIR/plain \
	-compression block \
	-language en
export PATH=$ORIGINAL_PATH
echo -e "Preprocess - WikiToPlainText\t\tDONE"

