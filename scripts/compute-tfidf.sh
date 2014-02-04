#!/bin/bash

DIR=$1
PARTITIONS=$2

mahout seq2sparse \
	-Dmapred.reduce.tasks=$PARTITIONS \
	-i $DIR/plain \
	-o $DIR/tfidf \
	-n 2 \
	-nv \
	--numReducers $PARTITIONS \

