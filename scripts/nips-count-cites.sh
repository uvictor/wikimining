#!/bin/bash

source vars.sh

java -cp $JAR \
	ch.ethz.las.wikimining.evaluate.NipsCiteCounter \
	../nips/influence-selected/part-00000 \
  ../nipstxt/benyah/
#  ../nipstxt/benyah/
#	../nipstxt/knowceans-ilda/nips/

