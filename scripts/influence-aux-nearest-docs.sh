#!/bin/bash

source vars.sh

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.NearestDocs \
	-Dmapred.reduce.tasks=$REDUCERS \
	-libjars $LIBJARS \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/lsh \
	-dimensions $DIMENSIONS
echo -e "Influence - Locality Sensitive Hashing\t\tDONE"
export PATH=$ORIGINAL_PATH

