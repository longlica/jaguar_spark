#!/bin/bash

export LD_LIBRARY_PATH=$HOME/jaguar/lib
JAR=$HOME/jaguar/lib/jaguar-jdbc-2.0.jar
PROJJAR=$HOME/jaguar_spark/target/scala-2.10/testjdbc_2.10-1.0.jar
export SPARK_HOME=$HOME/spark

date
t1=`date +'%s'`

echo "run $1"

$HOME/spark/bin/spark-submit \
    --class $1 \
    --jars $JAR \
    --master local[1] \
    --driver-library-path $LD_LIBRARY_PATH \
    $PROJJAR

t2=`date +'%s'`
((dt=t2-t1))
date
echo "Total $t1 --- $t2  $dt seconds"