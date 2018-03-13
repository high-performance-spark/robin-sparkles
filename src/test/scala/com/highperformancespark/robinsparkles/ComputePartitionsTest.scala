package com.highperformancespark.robinsparkles


import org.apache.spark.SparkConf
import org.scalatest.FunSuite


class ComputePartitionsTest extends FunSuite {

  test("If partitions increased and stage  time decrease, then we should increase partitions "){

    val first = WebUIInput(
      105,
      1024*1024*1024*2*3,
      3,
      List.fill(19)(Task(11)))

    val second = WebUIInput(
      90,
      1024*1024*1024*3,
      3,
      List.fill(20)(Task(10)))

    implicit val sparkConf =  new SparkConf().setAll(Map(
      "spark.executor.instances" -> "4",
      "spark.executor.cores" -> "2",
      "spark.executor.memory" -> s"${Math.round(1024*2/(0.6*0.5))}m"
    ))

    val partitions = ComputePartitions().fromStageMetric(List(first, second))
    assert(partitions > second.numPartitionsUsed)
  }

}