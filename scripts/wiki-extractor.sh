#!/bin/bash

source vars.sh

DIR=../tmp/plain
mkdir -p $DIR

time java -cp $JAR \
	ch.ethz.las.wikimining.evaluate.WikiExtractor \
  $DIR/part-m-00000 \
  $@
