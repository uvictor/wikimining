#!/bin/bash

source vars.sh
OUTPUT_DIR=$DIR/random-selected

mkdir -p $OUTPUT_DIR

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.evaluate.WikiRandomSelect \
	$DIR/tfidf/tfidf-vectors \
  $OUTPUT_DIR \
  $SELECT
echo -e "Wiki Random - Select\t\tDONE"
export PATH=$ORIGINAL_PATH

