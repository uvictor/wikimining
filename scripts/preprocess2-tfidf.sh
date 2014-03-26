#!/bin/bash

source vars.sh

# Mahout currently works only with Hadoop 1
export PATH=$MAHOUT_PATH
time mahout seq2sparse \
	-Dmapred.reduce.tasks=$REDUCERS \
	-i $DIR/plain \
	-o $DIR/tfidf \
  --analyzerName org.apache.lucene.analysis.en.EnglishAnalyzer \
	--numReducers $REDUCERS \
	--weight tfidf \
	--minSupport 6 \
	--minDF 3 \
  --maxDFSigma 3.0 \
	--norm 2 \
	--overwrite \
	--namedVector
export PATH=$ORIGINAL_PATH
echo -e "Preprocess - Mahout TfIdf\t\tDONE"

