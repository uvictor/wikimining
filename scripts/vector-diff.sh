#!/bin/bash

source vars.sh

java -cp $JAR \
	ch.ethz.las.wikimining.VectorComparator \
  ../nipstxt/clean/doclist.txt \
  ../nips/influence-novelty/part-00000 \
  ../tcov/novel.txt

#  ../nips/tfidf/tfidf-vectors/part-r-00000 \
#  ../nips/influence-novelty/part-00000 \

#  ../tcov/unigram_augmented.txt
#  ../tcov/novel.txt

