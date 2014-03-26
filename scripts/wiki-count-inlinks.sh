#!/bin/bash

source vars.sh

time java -cp $JAR \
	ch.ethz.las.wikimining.evaluate.WikiCountInlinks \
	$DIR/revisions-subset/part-00000 \
  $DIR/$NAME-${TYPE}.txt

