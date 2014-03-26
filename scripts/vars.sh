# Variables to be use by all other scripts.

PROJECT=~/NetBeansProjects/WikiMining/target
JAR=$PROJECT/WikiMining-1.0-SNAPSHOT.jar

NAME=ml2012
TYPE=rev-count-inlinks-cov-df-sqrt
DIR=../$NAME
MASTER_DIR=../20121001
XML=enwiki-20131104-smallest.xml

REDUCERS=1
PARTITIONS=1
SELECT=100
ROWS=13
BANDS=9
DIMENSIONS=2507

ORIGINAL_PATH="$PATH"
HADOOP_PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin"
HADOOP_TWO_PATH="$PATH:/home/uvictor/hadoop-2.2.0/bin"
MAHOUT_PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin:/home/uvictor/mahout/bin"
# Changes the way seqdirectory forms the key (used for the NIPS dataset)
MAHOUT_SRC_PATH="$PATH:/home/uvictor/hadoop-1.0.4/bin:/home/uvictor/mahout-src/bin"

LIBJARS="$JAR,$PROJECT/dependency/mahout-core-0.8-job.jar,$PROJECT/dependency/mahout-math-0.8.jar,$PROJECT/dependency/commons-cli-2.0-mahout.jar"

#export HADOOP_CLASSPATH="$PROJECT/dependency/mahout-core-0.8-job.jar:$PROJECT/dependency/mahout-math-0.8.jar:$PROJECT/dependency/commons-cli-2.0-mahout.jar"

echoerr() {
  echo "$@" 1>&2;
}

