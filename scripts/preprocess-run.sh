#!/bin/bash

source default.vars

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

