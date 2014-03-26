#!/bin/bash

source vars.sh

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.TfIdfWordSpread \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/influence-wordspread \
	-dates $DIR/date
echo -e "Influence - TfIdf Word Spread\t\tDONE"
export PATH=$ORIGINAL_PATH

