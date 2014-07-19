#!usr/bin/env python

from pyspark import SparkContext, SparkConf
import os
import sys
import random as rand

if len(sys.argv) < 3:
  print "Please enter the number of cores (partitions) and the scale size"
  exit(1)

numNodes = int(sys.argv[1])
scale = int(sys.argv[2])

aws_ak = os.environ.get("AWS_ACCESS_KEY_ID")
if aws_ak == None:
  print "Error! Please enter (export) your AWS Access Key Id"
  exit(2)

aws_sak = os.environ.get("AWS_SECRET_ACCESS_KEY")
if aws_sak == None:
  print "Error! Please enter (export) your AWS Secret Access Key"
  exit(3)

master_pub = os.environ.get("SPARK_MASTER_IP")
if master_pub == None:
  print "Error! Please enter (export) the public DNS of the master node"
  exit(4)

master = os.environ.get("MASTER")

print "MASTER: " + master
print "MASTER_PUB: " + master_pub

#def mitosis(scale,rat):
#  (uId,pId,rating) = rat
#  return [(uId+i, pId, rating) for i in range(scale)]
intmax = 2 ** 31 - 1
def extractRating(line):
  (uId,pId,rating) = line.split(" ")
  return (abs(hash(uId) % intmax), abs(hash(pId) % intmax), rating)

sconf = SparkConf().setAppName("Scale_Amazon_Dataset").setMaster(master)
sc = SparkContext(conf=sconf)

allData = sc.textFile("s3n://%s:%s@databricks-meng/amazon-reviews" % (aws_ak,aws_sak),numNodes).map(extractRating)

scaledData = (allData
                .flatMap(lambda rat: [(rand.random(), str(rat[0]+i) + " " + str(rat[1]) + " " + rat[2]) for i in range(scale)])
                .cache())


print "Total number of rows:" + str(scaledData.count())
print scaledData.first()


train = scaledData.filter(lambda value: value[0]<0.8).map(lambda (rnd, value): value).cache()
test = scaledData.filter(lambda value: value[0]>=0.8).map(lambda (rnd, value): value).cache()

print "# of training examples: %d, # of test examples: %d" % (train.count(), test.count())

print "Saving Training File..."

train.saveAsTextFile("hdfs://%s:9000/amatrain.train" % (master_pub))

print "Saved Training File. Now saving Test File..."

test.saveAsTextFile("hdfs://%s:9000/amatest.validate" % (master_pub))

print "Saved Test File. Finished run"

exit(0)


