# jaguar_spark

## Install sbt (under root)

$ wget http://dl.bintray.com/sbt/rpm/sbt-0.13.5.rpm

$ sudo yum localinstall sbt-0.13.5.rpm

## Install scala

$ wget http://downloads.typesafe.com/scala/2.11.7/scala-2.11.7.tgz

$ tar xvf scala-2.11.7.tgz

$ udo mv scala-2.11.7 /usr/lib

$ sudo ln -s /usr/lib/scala-2.11.7 /usr/lib/scala

$ export PATH=$PATH:/usr/lib/scala/bin

## Install Spark

$ wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.0-bin-hadoop2.6.tgz

$ tar xvf spark-1.6.0-bin-hadoop2.6.tgz

$ ln -s spark-1.6.0-bin-hadoop2.6 spark

## Build & Run

$ sbt clean package

$ bin/submit_sparkt.sh
