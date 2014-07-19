#!bin/bash

#Set CORES as nodes * cpu and SCALE as how much you want to scale your test dataset

export NODES=8
export CPUS_PER_NODE=8
let CORES=$NODES*$CPUS_PER_NODE

export ALL_SCALE=( 2 4 16 32 64 )

export DIR=$PWD
#Setup Powergraph script. Sets up Powergraph on a Spark EC2 cluster. Comment this line out if you already have
#Powergraph or are running the experiment on a local machine. Please adjust the Powergraph variable to the directory
#of Powergraph if you are skipping this step.

bash powergraph-setup.sh

#Setup Mahout
bash mahout-setup.sh

#startup hdfs and mapreduce
~/spark-ec2/copy-dir /root/ephemeral-hdfs
/root/spark/sbin/stop-all.sh
sleep 5
/root/spark/sbin/start-all.sh
sleep 5

~/ephemeral-hdfs/bin/start-mapred.sh
sleep 3
/root/ephemeral-hdfs/bin/start-dfs.sh
sleep 3

~/ephemeral-hdfs/bin/hadoop dfsadmin -safemode leave

export SPARK=~/spark
export POWERGRAPH=/mnt/powergraph/release/toolkits/collaborative_filtering
export MAHOUT=/mnt/mahout


source $SPARK/conf/spark-env.sh

#Default set for m3.2xlarge instances on EC2
export D_MEMORY=20g

#You can change this if you don't have hdfs or want to run locally.
export OUT_DIR=hdfs://$SPARK_MASTER_IP:9000

#Download netflix datasets for example run
cd /mnt
mkdir data
cd data
wget http://www.select.cs.cmu.edu/code/graphlab/datasets/smallnetflix_mm.train .
wget http://www.select.cs.cmu.edu/code/graphlab/datasets/smallnetflix_mm.validate .

cat small* >> input.txt

cd ~
spark-ec2/copy-dir /mnt/data

#Set here for your input file. Input file should be of form "userID productID rating" The delimiter can
#be anything, just specify it in the code shown below
#export INPUT=/mnt/data/smallnetflix_mm.train
#export VALIDATION=/mnt/data/smallnetflix_mm.validate
export INPUT=/mnt/data/input.txt
cd ~
mkdir testFilesALS
cd testFilesALS
mkdir spark
mkdir powergraph
mkdir mahout

cd $DIR

#Initialize Variables
export ITER=5
export ALL_RANK=( 10 20 40 )
export LAMBDA=0.065
export TRIALS=3

export HADOOP_HOME="/root/ephemeral-hdfs"
export HADOOP_CONF_DIR=~/mapreduce/conf
export PATH=$HADOOP_HOME:$HADOOP_HOME/bin:$PATH
export CLASSPATH=$(hadoop classpath)
export MIN_RATING=0
export MAX_RATING=5

for SCALE in ${ALL_SCALE[@]}
do
  ~/spark/sbin/start-all.sh
  sleep 3
  ~/ephemeral-hdfs/bin/hadoop dfs -rmr /bm_t*

##Scale Dataset and save in hdfs

#A delimiter to split data can be added by --del. Default is a whitespace separator
#If a second input is specified, datasets will just be copied to hdfs, scaling will not occur
$SPARK/bin/spark-submit --class ScaleDataset --driver-memory $D_MEMORY \
  $DIR/ALSBenchmarkSpark/target/scala-2.10/ALSBenchmark-assembly-1.0.jar \
  --cores $CORES --scale $SCALE --local \
  --input $INPUT > scaleOutLog-s_$SCALE.txt; export exit=$?

##Run spark ALS

if [ $exit = 0 ]; then

cd ~/testFilesALS/spark

for RANK in ${ALL_RANK[@]}
do
i=0
while [ $i -lt $TRIALS ]
do
  $SPARK/bin/spark-submit --class ALSBenchmark --driver-memory $D_MEMORY \
    $DIR/ALSBenchmarkSpark/target/scala-2.10/ALSBenchmark-assembly-1.0.jar \
    --numIterations $ITER --rank $RANK --lambda $LAMBDA --numBlocks $CORES \
    --kryo \
    $OUT_DIR 2>&1 | tee -a s_$SCALE-c_$CORES-d_$RANK-l_$LAMBDA-t_$ITER-$i.txt
  let i=i+1
done

done
fi

#Stop Spark memory consumption
/root/spark/sbin/stop-all.sh

##Run ALS on Powergraph

if [ $exit = 0 ]; then

  cd ~/testFilesALS/powergraph

 
  for RANK in ${ALL_RANK[@]}
  do
    let i=0
    while [ $i -lt $TRIALS ]
    do
      mpiexec -n $NODES --hostfile /root/spark-ec2/slaves -x CLASSPATH -x POWERGRAPH -x SPARK_MASTER_IP $POWERGRAPH/als \
        --ncpus $CPUS_PER_NODE --matrix $OUT_DIR/bm_train.train \
        --test $OUT_DIR/bm_test.validate \
        --max_iter $ITER --D $RANK --lambda $LAMBDA --tol 0.0000000001 \
        --minval $MIN_RATING --maxval $MAX_RATING 2>&1 | tee -a s_$SCALE-c_$CORES-d_$RANK-l_$LAMBDA-t_$ITER-$i.txt
      let i=i+1

  ###or if you want to run locally
  #
  #$POWERGRAPH/als --matrix $OUT_DIR/amatrain.train --test $OUT_DIR/amatest.validate \
  #--max_iter $ITER --D $RANK --lambda $LAMBDA --regnormal 0 --tol 0.0000000001 > \
  #s_$SCALE-c_$CORES-d_$RANK-l_$LAMBDA-t_$ITER-$i
  #
    done
  done
cd ..

fi

## Run ALS on Mahout

if [ $exit = 0 ]; then

  cd ~/testFilesALS/mahout
  export MEM=2g

  for RANK in ${ALL_RANK[@]}; do
    let i=0
    while [ $i -lt $TRIALS ]
    do
      $HADOOP_HOME/bin/hadoop dfs -rmr /user/root/t*

      { time $MAHOUT/bin/mahout parallelALS --input hdfs://$SPARK_MASTER_IP:9000/bm_train.train \
        --lambda $LAMBDA --numFeatures $RANK --numIterations $ITER --numThreadsPerSolver $CPUS_PRE_NODE \
        --output tmp-$i -Dmapred.map.tasks=$CORES -Dmapred.reduce.tasks=$CORES -Dmapred.child.java.opts=-Xmx$MEM \
        2>&1 | tee -a s_$SCALE-c_$CORES-d_$RANK-l_$LAMBDA-t_$ITER-$i.txt ; } \
        2> s_$SCALE-time-c_$CORES-d_$RANK-l_$LAMBDA-t_$ITER-$i.txt
      let i=i+1
    done

  done

fi

done


