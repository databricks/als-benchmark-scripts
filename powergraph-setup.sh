#!/usr/bin/env bash

#### Setup graphlab

# Some options that were tried:
#  - building with tcmalloc leads to seg fault 13 on Mac OS X, and build failure on EC2
#  - input was split into disjoint parts to maximize GraphLab load speed (not required for Spark or Mahout)
#  - disabling dynamic graph leads to build failure on EC2
#  - maxval / minval was set according to
#   http://bickson.blogspot.com/2013/03/intel-labs-report-on-graphlab-vs-mahout.html


# install dependencies across cluster
yum install -y openmpi-devel zlib-devel cmake
ln -s /usr/lib64/openmpi/bin/* /usr/bin/.
~/ephemeral-hdfs/bin/slaves.sh yum install -y openmpi-devel zlib-devel cmake
~/ephemeral-hdfs/bin/slaves.sh ln -s /usr/lib64/openmpi/bin/* /usr/bin/.

export GL_MASTER=https://github.com/graphlab-code/graphlab.git
export GL_BRANCH=https://github.com/brkyvz/graphlab.git
# Master of graphlab
git clone $GL_MASTER /mnt/powergraph
cd /mnt/powergraph
git remote add brkyvz $GL_BRANCH
git fetch brkyvz
git merge brkyvz/mod

# make on /mnt because compiling requires a bunch of disk space
# and we kept running out on the 8GB ec2 root volume

#git checkout spark-ec2-build

./configure --no_tcmalloc
# we only needed to build the graph-analytics toolkit. Adjust this to build
# the toolkits you care about. ./configure outputs instructions on this too.
cd release/toolkits/collaborative_filtering
# -j8 means use parallel compilation with 8 threads. Adjust to the number of cores you have available.
make -j8
cd /mnt
# copy binary to all nodes in cluster
~/spark-ec2/copy-dir /mnt/powergraph

cd ~/
#get pip to install awscli, install aws for easy bucket access
#wget https://bootstrap.pypa.io/get-pip.py .
#python get-pip.py
#sudo pip install awscli

#some export commands to run graphlab more easily not required by this script
#export HADOOP_HOME="/root/ephemeral-hdfs"
#export PATH=$HADOOP_HOME:$HADOOP_HOME/bin:$PATH
#export CLASSPATH=$(hadoop classpath)
#export GRAPHLAB=/mnt/graphlab/release/toolkits/collaborative_filtering


#Sync s3 bucket here

#Run als
#========

#Before running, don't forget to sync s3 bucket and copy them to hdfs

#mpiexec -n 64 --hostfile /root/spark-ec2/slaves -x CLASSPATH -x GRAPHLAB -x MASTER $GRAPHLAB/als \
  #--matrix hdfs://$MASTER:9000/amazon-reviewsx8/amatrain.train \
  #--test hdfs://$MASTER:9000/amazon-reviewsx8/amatest.validate \
  #--max_iter 10 --D 10 --lambda 0.1 --regnormal 0 --tol 0.0000000001 > testx8-n_64
