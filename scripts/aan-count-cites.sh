#!/bin/bash

source vars.sh

java -cp $jar \
	ch.ethz.las.wikimining.AanCiteCounter \
	../aan/influence-selected/ \
	../aantxt/release/2012/paper_citations.txt

