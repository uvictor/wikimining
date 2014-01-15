#!/bin/bash

DIR=$1

# Mahout currently works only with Hadoop 1.2.1
export PATH="$PATH:/home/uvictor/hadoop-1.2.1/bin:/home/uvictor/mahout/bin"

mahout seq2sparse \
	-i $DIR/plain \
	-o $DIR/tfidf

