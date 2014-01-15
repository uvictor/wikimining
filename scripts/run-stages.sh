#!/bin/bash

JAR=~/NetBeansProjects/WikiMining/target/WikiMining-1.0-SNAPSHOT.jar
DIR=../20121001
XML=enwiki-20121001-pages-articles.xml

ORIGINAL_PATH="$PATH"
export PATH="$PATH:/home/uvictor/hadoop-2.2.0/bin"

./make-wiki-seq.sh $JAR \
	ch.ethz.las.wikimining.mr.WikiToPlainText \
	$DIR $XML

# Mahout currently works only with Hadoop 1.2.1
export PATH=$ORIGINAL_PATH
./compute-tfidf.sh $DIR
export PATH="$PATH:/home/uvictor/hadoop-2.2.0/bin"

./first-word-coverage.sh $JAR \
	ch.ethz.las.wikimining.mr.WordCoverageFirstGreeDi \
	$DIR

./second-word-coverage.sh $JAR \
	ch.ethz.las.wikimining.mr.WordCoverageSecondGreeDi \
	$DIR

export PATH=$ORIGINAL_PATH

