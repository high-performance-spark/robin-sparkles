package com.highperformancespark.robinsparkles

import java.nio.file.FileSystem

import com.highperformancespark.robinsparkles.CountingLocalApp.conf
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io
import scala.util.Try

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object CountingLocalApp extends App{

  val DEFAULT_INPUT_FILE = "src/test/resources/Words.txt"
  val DEFAULT_OUTPUT_FILE = "tmp/output"
  val DEFAULT_METRICS_DIR = "tmp/metrics"
  println("ARGS ARE " + args.mkString(", "))

  val inputFile = Try(args(0)).getOrElse{
    println(s"No input file arg provided using default $DEFAULT_INPUT_FILE")
    DEFAULT_INPUT_FILE}

  val outputFile = Try(args(1)).getOrElse{
    println(s"No output file provided, using default $DEFAULT_OUTPUT_FILE")
    DEFAULT_OUTPUT_FILE
  }

 val metricsDir = Try(args(3)).getOrElse{
   println(s"No metrics dir provided, using default $DEFAULT_METRICS_DIR")
    DEFAULT_METRICS_DIR
  }


  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my_awesome_app").set("spark.num.executors", "1")

  val (newConf: SparkConf, id: Int) = Runner.getOptimizedConf(metricsDir, conf)
  val sc = SparkContext.getOrCreate(newConf)

  Runner.run(sc, id, metricsDir, inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object CountingApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  Runner.run(SparkContext.getOrCreate(), 0, "/tmp", inputFile, outputFile)
}

object Runner {

  def getOptimizedConf(metricsDir: String, conf: SparkConf):
      (SparkConf, Int) = {
    val metricsReader = new MetricsReader(conf, metricsDir)
    // Load all of the previous runs until one isn't found
    val prevRuns = Stream.from(0)
      .map(id => metricsReader.getRunInfo(id))
      .takeWhile(_.isDefined)
      .map(_.get)

    prevRuns.zipWithIndex.foreach(x=> println(x._2 + x._1.mkString(", ")))
    val partitions = ComputePartitions(conf)
      .fromStageMetricSharedCluster(
      StageInfo.stagesWithMostExpensiveShuffle(prevRuns)
    )


   conf.set("spark.default.parallelism", partitions.toString)

    println(s"Found ${prevRuns.length} runs of historical data in metrics dir $metricsDir")

    println("Optimized conf is: --------------------------")
    println(conf.getAll.mkString("\n"))
    (conf, prevRuns.length)
  }

  def run(sc: SparkContext, id: Int, metricsDir: String, inputFile: String, outputFile: String): Unit = {

    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    if(fs.exists(new Path(outputFile))){
      println(s"Output path $outputFile already exists, deleting it" )
      fs.delete(new Path(outputFile), true)
    }
    val metricsCollector = new MetricsCollector(sc, metricsDir)
    metricsCollector.startSparkJobWithRecording(id)

    val rdd = sc.textFile(inputFile)
    val counts = WordCount.withStopWordsFiltered(rdd)

    counts.saveAsTextFile(outputFile)
  }
}
