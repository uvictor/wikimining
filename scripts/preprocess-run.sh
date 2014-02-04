#!/bin/bash

JAR=~/NetBeansProjects/WikiMining/target/WikiMining-1.0-SNAPSHOT.jar
DIR=../smallest
XML=enwiki-20131104-smallest.xml
#DIR=../20121001
#XML=enwiki-20121001-pages-articles.xml
PARTITIONS=10

ORIGINAL_PATH="$PATH"
HADOOP_PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin"
HADOOP_ONE_PATH="$PATH:/home/uvictor/hadoop-1.2.1/bin"
HADOOP_TWO_PATH="$PATH:/home/uvictor/hadoop-2.2.0/bin"
MAHOUT_PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin:/home/uvictor/mahout/bin"

echoerr() {
  echo "$@" 1>&2;
}

export PATH=$HADOOP_TWO_PATH
time ./make-wiki-seq.sh $JAR \
	ch.ethz.las.wikimining.mr.coverage.WikiToPlainText \
	$DIR $XML
export PATH=$ORIGINAL_PATH
echo -e "Preprocess - Wiki -> Plain\t\tDONE"

# Mahout currently works only with Hadoop 1
export PATH=$MAHOUT_PATH
time ./compute-tfidf.sh $DIR $PARTITIONS
export PATH=$ORIGINAL_PATH
echo -e "Preprocess - Mahout TfIdf\t\tDONE"

