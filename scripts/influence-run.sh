#!/bin/bash

JAR=~/NetBeansProjects/WikiMining/target/WikiMining-1.0-SNAPSHOT.jar
DIR=../smallest
XML=enwiki-20131104-smallest.xml
DIMENSIONS=155444
#DIR=../20121001
#XML=enwiki-20121001-pages-articles.xml
#DIMENSIONS=155444
PARTITIONS=10
SELECT=4

ORIGINAL_PATH="$PATH"
HADOOP_PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin"
HADOOP_TWO_PATH="$PATH:/home/uvictor/hadoop-2.2.0/bin"

echoerr() {
  echo "$@" 1>&2;
}

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.DocumentDate \
	-input $DIR/$XML \
	-output $DIR/date
echo -e "Influence - Document Date\t\tDONE"
export PATH=$ORIGINAL_PATH

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.TfIdfNovelty \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/influence-tfidf \
	-dates $DIR/date \
	-dimensions $DIMENSIONS
echo -e "Influence - TfIdf Novelty (LSH)\t\tDONE"
export PATH=$ORIGINAL_PATH

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.TfIdfWordSpread \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/influence-wordspread \
	-dates $DIR/date
echo -e "Influence - TfIdf Word Spread\t\tDONE"
export PATH=$ORIGINAL_PATH

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.GreeDiFirst \
	-input $DIR/tfidf/tfidf-vectors \
	-output $DIR/influence-subset \
	-dates $DIR/date \
	-spread $DIR/influence-wordspread \
	-partitions $PARTITIONS \
	-select $SELECT
echo -e "Influence - GreeDi First Pass\t\tDONE"
export PATH=$ORIGINAL_PATH

export PATH=$HADOOP_TWO_PATH
time hadoop jar $JAR \
	ch.ethz.las.wikimining.mr.influence.GreeDiSecond \
	-input $DIR/tfidf/tfidf-vectors \
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

