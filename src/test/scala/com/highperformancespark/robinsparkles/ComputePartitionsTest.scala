package com.highperformancespark.robinSparkles

import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class ComputePartitionsTest extends FunSuite {

  test("Results are resonable"){

    val first = new WebUIInput(
      105, List.fill[Int](19)(11).map(new Task(_)),
      List.fill[Long](3)(1024*1024*1024*2).map(new ExecutorSummary(_)))

    val second = new WebUIInput(
      90, List.fill(20)(10).map(Task(_)),
      Array.fill(3)(1024*1024*1024).map(ExecutorSummary(_)).toList)

    implicit val sparkConf =  new SparkConf().setAll(Map(
      "spark.executor.instances" -> "4",
      "spark.executor.cores" -> "2",
      "spark.executor.memory" -> s"${Math.round(1024*2/(0.6*0.5))}m"
    ))

    val partitions = ComputePartitions().fromStageMetric(List(first, second))
    assert(partitions > second.numPartitionsUsed)

  }

}
