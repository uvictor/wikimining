#!/bin/bash

JAR=$1
CLASS=$2
DIR=$3
SELECT=$4
PARTITIONS=$5

hadoop jar \
	$JAR \
	$CLASS \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/coverage-subset \
	-select $SELECT \
	-partitions $PARTITIONS

