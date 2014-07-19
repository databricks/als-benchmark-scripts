#!usr/bin/bash

cd /mnt
mkdir maven
cd maven

wget http://mirrors.sonic.net/apache/maven/maven-3/3.2.2/binaries/apache-maven-3.2.2-bin.tar.gz
tar -xzvf apache-maven-3.2.2-bin.tar.gz

export PATH=/mnt/maven/apache-maven-3.2.2/bin:$PATH
export MAHOUT=/mnt/mahout
export MAVEN_OPTS=-Xmx1024m
export HADOOP_HOME=~/ephemeral-hdfs
export HADOOP_CONF_DIR=~/mapreduce/conf
export AM_BRANCH=https://github.com/brkyvz/mahout.git
cd ..
git clone https://github.com/apache/mahout.git mahout
cd mahout

git remote add brkyvz $AM_BRANCH
git fetch brkyvz
git merge brkyvz/mod


cd $MAHOUT
mvn -DskipTests clean install

$HADOOP_HOME/bin/start-all.sh

cd ~

spark-ec2/copy-dir /mnt/mahout

for line in `cat ~/spark/conf/slaves`; do
  echo $line
  ssh -t -o StrictHostKeyChecking=no root@$line 'echo 1 > /proc/sys/vm/overcommit_memory'
done



