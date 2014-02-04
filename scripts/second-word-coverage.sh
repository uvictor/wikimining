#!/bin/bash

JAR=$1
CLASS=$2
DIR=$3
SELECT=$4

hadoop jar $JAR $CLASS \
	-input $DIR/tfidf/tfidf-vectors \
	-docs $DIR/coverage-subset \
	-output ../smallest/coverage-selected \
	-select $SELECT

