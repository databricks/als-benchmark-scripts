/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.esotericsoftware.kryo.Kryo
import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}

/**
 * An example app for ALS on MovieLens data (http://grouplens.org/datasets/movielens/).
 * Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.MovieLensALS
 * }}}
 * A synthetic dataset in MovieLens format can be found at `data/mllib/sample_movielens_data.txt`.
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object ALSBenchmark {

  class ALSRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Rating])
    }
  }

  case class Params(
      input: String = null,
      kryo: Boolean = false,
      numIterations: Int = 20,
      numBlocks: Int = 2,
      lambda: Double = 1.0,
      rank: Int = 10,
      implicitPrefs: Boolean = false,
      delimiter: String= "\\s++"
    )

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ALSBenchmark") {
      head("ALS Benchmark: an example app for ALS on Amazon data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Int]("numBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numBlocks} (auto)")
        .action((x, c) => c.copy(numBlocks = x))
      opt[Unit]("kryo")
        .text(s"use Kryo serialization")
        .action((_, c) => c.copy(kryo = true))
      opt[Unit]("implicitPrefs")
        .text("use implicit preference")
        .action((_, c) => c.copy(implicitPrefs = true))
      opt[String]("del")
        .text("specify custom delimiter for reading the dataset. Default: whitespace")
        .action((x, c) => c.copy(delimiter = x))
      arg[String]("<input>")
        .required()
        .text("input paths to a Amazon dataset of ratings")
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName("ALSBenchmark with " + params.rank + ", " + params.numIterations + ", " +
                  params.lambda)
    if (params.kryo) {
      conf.set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[ALSRegistrator].getName)
        .set("spark.kryoserializer.buffer.mb", "8")
    }
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)
    
    val trainPre = sc.textFile(params.input + "/bm_train.train",8)
    val training = trainPre.map { line =>
      val f = line.split(params.delimiter)
      if (params.implicitPrefs) {
        Rating(f(0).toInt, f(1).toInt, f(2).toDouble-2.5)
      } else{
        Rating(f(0).toInt, f(1).toInt, f(2).toDouble)
      }
    }.repartition(params.numBlocks).cache()
    
    val test = sc.textFile(params.input + "/bm_test.validate", 8).map { line =>
      val f = line.split(params.delimiter)
      if (params.implicitPrefs) {
      /*
       * 0 means "don't know" and positive values mean "confident that the prediction should be 1".
       * Negative values means "confident that the prediction should be 0".
       * We have in this case used some kind of weighted RMSE. The weight is the absolute value of
       * the confidence. The error is the difference between prediction and either 1 or 0,
       * depending on whether r is positive or negative.
       */
        Rating(f(0).toInt, f(1).toInt, if (f(2).toDouble-2.5 > 0) 1.0 else 0.0)
      } else {
        Rating(f(0).toInt, f(1).toInt, f(2).toDouble)
      }
    }.cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    val start = System.currentTimeMillis()
    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .run(training)
    val end = System.currentTimeMillis()

    println("Train Time = " + (end-start)*1.0/1000)
    val rmse = computeRmse(model, test, params.implicitPrefs)
    println(s"Train RMSE = " + computeRmse(model, training,params.implicitPrefs))
    println(s"Test RMSE = $rmse.")

    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean) = {

    def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }
}
