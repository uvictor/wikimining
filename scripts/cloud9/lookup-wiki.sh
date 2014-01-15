#!/bin/bash

DIR=~/data/smallest
DOCNO=$DIR/docno.cloud9
INDEX=$DIR/index.cloud9

ORIGINAL_PATH="$PATH"
export PATH="$PATH:/home/uvictor/hadoop-2.2.0/bin"

etc/hadoop-cluster.sh \
  edu.umd.cloud9.collection.wikipedia.LookupWikipediaArticle \
  $INDEX \
  $DOCNO

export PATH=$ORIGINAL_PATH

