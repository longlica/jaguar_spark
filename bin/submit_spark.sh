#!/bin/bash

/bin/rm -rf /tmp/sparkout.txt

export LD_LIBRARY_PATH=$HOME/jaguar/lib
JAR=$HOME/jaguar/lib/jaguar-jdbc-2.0.jar
PROJJAR=/home/yzhj/spark1.5/TestJDBC/target/scala-2.10/testjdbc_2.10-1.0.jar
export SPARK_HOME=/home/yzhj/spark1.5/spark

date
t1=`date +'%s'`

/home/dev2/spark1.5/spark/bin/spark-submit \
	--class TestScalaJDBC \
	--jars $JAR \
    --master local[1] \
	--driver-library-path $LD_LIBRARY_PATH \
    $PROJJAR

t2=`date +'%s'`
((dt=t2-t1))
date
echo "Total $t1 --- $t2  $dt seconds"
