#!/bin/bash

source vars.sh

time ./hadoop-seq.sh $DIR/plain/part-m-00000 \
  | cut -f1 -s > $DIR/ids.txt

