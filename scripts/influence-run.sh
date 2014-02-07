#!/bin/bash

source default.vars

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.TfIdfNovelty \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/influence-dup-novelty \
	-basis $DIR/influence-basis-matrix \
	-dates $DIR/date
echo -e "Influence - TfIdf Novelty (LSH)\t\tDONE"
export PATH=$ORIGINAL_PATH

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.TfIdfRemoveDuplicates \
	-input $DIR/influence-dup-novelty \
	-output $DIR/influence-novelty
echo -e "Influence - TfIdf Remove Duplicates\t\tDONE"
export PATH=$ORIGINAL_PATH

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.TfIdfWordSpread \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/influence-wordspread \
	-dates $DIR/date
echo -e "Influence - TfIdf Word Spread\t\tDONE"
export PATH=$ORIGINAL_PATH

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.GreeDiFirst \
	-input $DIR/influence-novelty \
	-output $DIR/influence-subset \
	-dates $DIR/date \
	-spread $DIR/influence-wordspread \
	-partitions $PARTITIONS \
	-select $SELECT
echo -e "Influence - GreeDi First Pass\t\tDONE"
export PATH=$ORIGINAL_PATH

export PATH=$HADOOP_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.h104.GreeDiSecond \
	-input $DIR/influence-novelty \
	-output $DIR/influence-selected \
	-dates $DIR/date \
	-spread $DIR/influence-wordspread \
	-docs $DIR/influence-subset \
	-select $SELECT
echo -e "Influence - GreeDi Second Pass\t\tDONE"
export PATH=$ORIGINAL_PATH



#export PATH=$HADOOP_TWO_PATH
#time hadoop jar $JAR \
#	ch.ethz.las.wikimining.mr.influence.NearestDocs \
#	-input $DIR/tfidf/tfidf-vectors \
#	-output $DIR/lsh \
#	-dimensions $DIMENSIONS
#echo -e "Influence - Locality Sensitive Hashing\t\tDONE"
#export PATH=$ORIGINAL_PATH

