package com.highperformancespark.robinsparkles
//import com.highperformancespark.robinsparkles.listener._

/**
 * A simple test for everyone's favourite wordcount example.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class WordCountTest extends FunSuite with DataFrameSuiteBase {
  test("word count with Stop Words Removed"){
   // val receiver = Receiver(spark)

    val linesRDD = sc.parallelize(Seq(
      "How happy was the panda? You ask.",
      "Panda is the most happy panda in all the#!?ing land!"))

    val stopWords: Set[String] = Set("a", "the", "in", "was", "there", "she", "he")
    val splitTokens: Array[Char] = "#%?!. ".toCharArray

    val wordCounts = WordCount.withStopWordsFiltered(
      linesRDD, splitTokens, stopWords)
    val wordCountsAsMap = wordCounts.collectAsMap()
    assert(!wordCountsAsMap.contains("the"))
    assert(!wordCountsAsMap.contains("?"))
    assert(!wordCountsAsMap.contains("#!?ing"))
    assert(wordCountsAsMap.contains("ing"))
    assert(wordCountsAsMap.get("panda").get.equals(3))
  //  val metricsDF = receiver.df()
  //  assert(metricsDF.count() === 3)
  }
}
