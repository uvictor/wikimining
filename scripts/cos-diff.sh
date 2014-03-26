#!/bin/bash

source vars.sh

java -cp $JAR \
	ch.ethz.las.wikimining.CosineDiff \
	../nips/tfidf/tfidf-vectors/part-r-00000 \
  ../tcov/docDistance.txt

