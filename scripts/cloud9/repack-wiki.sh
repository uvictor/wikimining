#!/bin/bash

DIR=~/data/smallest
XML=$DIR/enwiki-20131104-smallest.xml
DOCNO=$DIR/docno.cloud9
BLOCK=$DIR/block.cloud9
INDEX=$DIR/index.cloud9

ORIGINAL_PATH="$PATH"
export PATH="$PATH:/home/uvictor/hadoop-2.2.0/bin"

etc/hadoop-cluster.sh \
  edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMappingBuilder \
  -input $XML \
  -output_file $DOCNO \
  -wiki_language en \
  -keep_all

etc/hadoop-cluster.sh \
  edu.umd.cloud9.collection.wikipedia.RepackWikipedia \
  -input $XML \
  -mapping_file $DOCNO \
  -output $BLOCK \
  -wiki_language en \
  -compression_type block

etc/hadoop-cluster.sh \
  edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndexBuilder \
  -input $BLOCK \
  -index_file $INDEX

etc/hadoop-cluster.sh \
  edu.umd.cloud9.collection.wikipedia.LookupWikipediaArticle \
  $INDEX \
  $DOCNO

export PATH=$ORIGINAL_PATH

