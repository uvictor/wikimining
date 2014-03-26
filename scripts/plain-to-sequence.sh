#!/bin/bash

source vars.sh

java -cp $JAR \
	ch.ethz.las.wikimining.VectorPlainToSequence \
  ../nipstxt/clean/doclist.txt \
  ../tcov/unigram_augmented.txt \
	../nips/tfidf/tfidf-vectors/part-r-00000

