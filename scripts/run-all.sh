#!/bin/bash

for NAME in "ml2012" "gameai" "vectors" "all3" "composers" "all4"
do
  echo $NAME
  ./revisions2-greedi-first.sh
  ./wiki-count-inlinks.sh
done

