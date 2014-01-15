#!/bin/bash

JAR=$1
CLASS=$2
DIR=$3
XML=$4

hadoop jar \
	$JAR \
	$CLASS \
	-input $DIR/$XML \
	-output $DIR/plain \
	-compression_type block \
	-wiki_language en

