#!/bin/bash

JAR=$1
CLASS=$2
DIR=$3

hadoop jar $JAR $CLASS \
	-input $DIR/tfidf/tfidf-vectors \
	-docs $DIR/subset \
	-output ../smallest/selected \
	-partitions 2

