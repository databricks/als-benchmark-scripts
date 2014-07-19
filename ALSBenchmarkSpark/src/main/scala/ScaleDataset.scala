

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

import scala.math.abs
import scopt.OptionParser

object ScaleDataset {

  case class Params(
    cores: Int = 2,
    scale: Int = 2,
    input: String= "",
    delimiter: String= "\\s++",
    secInput: String= "",
    local: Boolean= false
  )

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("ScaleDataset"){
      head("Scale the amazon reviews dataset")
      opt[Int]("cores")
        .required()
        .text("Number of cores in the cluster")
        .action((x, c) => c.copy(cores = x))
      opt[Int]("scale")
        .required()
        .text("The desired scale of the dataset (>1)")
        .action((x, c) => c.copy(scale = x))
      opt[String]("input")
        .required()
        .text("The input file")
        .action((x, c) => c.copy(input = x))
      opt[String]("del")
        .text("The delimiter to use for parsing the input file. Default: whitespace")
        .action((x, c) => c.copy(delimiter = x))
      opt[String]("test")
        .text("The test file")
        .action((x, c) => c.copy(secInput = x))
      opt[Unit]("local")
        .text("Load file locally, not hdfs")
        .action((_, c) => c.copy(local = true))
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"Scale dataset with scale: " + params.scale + ", cores: " + params.cores)
    val sc = new SparkContext(conf)

    val master_pub = System.getenv("SPARK_MASTER_IP")
    if (master_pub == None) {
      println("Error! Please enter (export) the public DNS of the master node")
      System.exit(2)
    }

    val output = System.getenv("OUT_DIR")
    if (output == None) {
      println("Error! Please enter (export) the desired output directory for your dataset")
      System.exit(3)
    }

    val master = System.getenv("MASTER")

    if (params.secInput != ""){

      var trainIn = params.input
      var testIn = params.secInput

      if (params.local){
        trainIn = "file://" + trainIn
        testIn = "file://" + testIn
      }

      val train = sc.textFile(trainIn).repartition(params.cores).cache()
      val test = sc.textFile(testIn).repartition(params.cores).cache()

      saveFiles(train, test, output)

    } else {

      var mainIn = params.input

      if (params.local){
        mainIn = "file://" + mainIn
      }
      val allData = sc.textFile(mainIn).map { line =>
        val f = line.split(params.delimiter)
        Rating(abs(f(0).##), abs(f(1).##), f(2).toDouble)
      }.repartition(params.cores)

      val numRatings = allData.count()
      val numUsers = allData.map(_.user).distinct().count()
      val numProducts = allData.map(_.product).distinct().count()

      println(s"Original Dataset: ")
      println(s"# of Ratings: $numRatings, # of Users: $numUsers, # of Products: $numProducts")

      val scaledData = allData.flatMap { rat =>
        def mitosis(v: Rating) = for (i <- 0 until params.scale) yield Rating(v.user+i, v.product, v.rating)
        mitosis(rat)
      }.cache()

      //there may be collisions due to taking the absolute value but it doesn't matter
      val numNewRatings = scaledData.count()
      val numNewUsers = scaledData.map(_.user).distinct().count()

      println(s"Scaled Dataset: ")
      println(s"# of ratings: $numNewRatings, # of Users: $numNewUsers, # of Products: $numProducts")

      val splits = scaledData.map { rat =>
        rat.user + " " + rat.product + " " + rat.rating
      }.randomSplit(Array(0.8,0.2))

      val train = splits(0).cache()
      val test = splits(1).cache()

      saveFiles(train, test, output)

    }



    sc.stop()
  }

  def saveFiles(train: RDD[String], test: RDD[String], output: String) {

    var trainOut = output
    var testOut = output

    if (output.endsWith("/")){
      trainOut += "bm_train.train"
      testOut += "bm_test.validate"
    }else {
      trainOut += s"/bm_train.train"
      testOut += s"/bm_test.validate"
    }


    println("# of training examples: " + train.count + ", # of test examples: " + test.count)
    println("Saving Training File...")

    train.saveAsTextFile(trainOut)
    println("Saved Training File. Now saving Test File...")

    test.saveAsTextFile(testOut)

    println("Saved Test File.")

  }
}
