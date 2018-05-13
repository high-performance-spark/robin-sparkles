package com.highperformancespark.robinsparkles

import ch.cern.sparkmeasure.{StageVals, TaskVals}
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

//These are placeholders for some kind of executor information or tasks class since those sparks objects are private
case class Task(totalTime: Int) //TODO Replace this with the actual stuff or spark object

/**
 * I have tried to put the logic for computing things from the UI metrics directly in this case clss so that we can be
 * flexible about which of those metrics we collect
 */
case class StageInfo(

  executorCPUTime: Int,
  stageTime: Int,
  totalInputSize : Double,
  numExectutors : Int,
  shuffleSize: ShuffleSize,
  private val taskMetrics: List[Task]){

  val totalTaskTime: Int = taskMetrics.foldRight(0)((t, b) => b + t.totalTime)

  val numPartitionsUsed: Int = taskMetrics.length

}

case class ShuffleSize(bytesWritten : Long, bytesRead: Long) extends Ordered[ShuffleSize] {
  override def compare(that: ShuffleSize) = bytesWritten.compareTo(that.bytesWritten)
}

object StageInfo{

  def apply(s : StageVals, tList : Seq[TaskVals]) : StageInfo = {

    val numExecutors = tList.map(_.executorId).distinct.size
    val shuffleSize = ShuffleSize(s.shuffleBytesWritten, s.shuffleTotalBytesRead)
    val inputSizeMb = s.bytesRead/(1024.0*1024.0)
    StageInfo(
      s.executorCpuTime.toInt, //This is the amount of time that the executor was actually doing the computation for this stage
      s.stageDuration.toInt,
      inputSizeMb,
      numExecutors,
      shuffleSize,
      tList.map(t => Task(t.duration.toInt)).toList)
  }

  def stagesWithMostExpensiveShuffle(previousRuns : Stream[List[StageInfo]]): List[StageInfo]= {
   previousRuns.headOption.map(_.zipWithIndex.maxBy(_._1.shuffleSize)) match {
     case Some((sageInfo, index)) => previousRuns.flatMap(w => List(w(index))).toList
     case None => List[StageInfo]()
   }
  }
}


case class ComputePartitions(val sparkConf: SparkConf) {
  implicit val logger: Logger = LoggerFactory.getLogger(classOf[ComputePartitions])
  final val TASK_OVERHEAD_MILLI = 10

  /**
   * Increases the number of partitions until the cluster if fully utalized according to stage duration OR the performance
   * stops improving
   * @param previousRuns
   * @return
   */
  def fromStageMetric(previousRuns: List[StageInfo]): Int = {
    val concurrentTasks = possibleConcurrentTasks()
    previousRuns match {
      case Nil => concurrentTasks
      case first :: Nil => (first.numPartitionsUsed + first.numExectutors)
      case first :: second :: _ =>
        val inputTaskSize = second.totalInputSize
        val taskMemoryMb = availableTaskMemoryMB()
        val floor: Int = Math.max(Math.round(inputTaskSize/taskMemoryMb).toInt, concurrentTasks)
        val execTime = executorIdleTime(second)
        if(execTime > 0){
          if(morePartitionsIsBetter(first,second)){
            Seq(floor, first.numPartitionsUsed, second.numPartitionsUsed).max + second.numExectutors
          }else{
            second.numPartitionsUsed
          }
        }else{
          //Wow that is incredible!
          logger.info("Wow!Your tasks are distributed amongst the executors with maximum efficiency. Don't change a thing")
          Math.max(floor, second.numPartitionsUsed)
        }
    }
  }

  def fromStageMetricSharedCluster(previousRuns: List[StageInfo]): Int = {
    val concurrentTasks = possibleConcurrentTasks()
    previousRuns match {
      case Nil =>
        //If this is the first run and parallelism is not provided, use the number of concurrent tasks
        sparkConf
          .getOption("spark.default.parallelism")
          .map(_.toInt)
          .getOrElse(concurrentTasks)
      case first :: Nil => first.numPartitionsUsed + math.max(first.numExectutors,1)
      case  _   =>
        val first = previousRuns(previousRuns.length - 2)
        val second = previousRuns(previousRuns.length - 1)
        val inputTaskSize = second.totalInputSize
        val taskMemoryMb = availableTaskMemoryMB()
        val floor: Int = Math.max(Math.round(inputTaskSize/taskMemoryMb).toInt, concurrentTasks)
        logger.info("Partitions --> executor CPU time")
        previousRuns.foreach(metrics => logger.info(s"${metrics.numPartitionsUsed} -> ${metrics.executorCPUTime}ms"))

        if(morePartitionsIsBetter(first,second)){
          //Increase the number of partitions
           Seq(floor, first.numPartitionsUsed, second.numPartitionsUsed).max + second.numExectutors
        }else{
          //If we overshot the number of partitions, use whichever run had the best executor cpu time
          logger.info("Increasing the number of partitions is not improving performance ")
          previousRuns.sortBy(_.executorCPUTime).head.numPartitionsUsed
        }
    }
  }

  //TODO: What is the best way to calculate this if using dynamic allocation
  def possibleConcurrentTasks(): Int = {
    sparkConf.getInt("spark.executor.cores", 1) * sparkConf.getInt("spark.num.executors", 1)
  }

  /**
   * The compute space on one executor devided by the number of cores
   * spark.executor.memory * spark.memory.fraction * (1-spark.storageFraction)/spark.executor.cores
   * @return
   */
  def availableTaskMemoryMB(): Double = {
    val memFraction = sparkConf.getDouble("spark.memory.fraction", 0.6)
    val storageFraction = sparkConf.getDouble("spark.memory.storageFraction", 0.5)
    val nonStorage = 1-storageFraction
    val cores = sparkConf.getInt("spark.executor.cores", 1) //We should probably fail here?
    Math.ceil(executorMemory * memFraction * nonStorage / cores)
  }

  lazy val executorMemory : Long = {
     Try(sparkConf.getSizeAsMb("spark.executor.memory"))
       .getOrElse(
         Option(System.getenv("SPARK_EXECUTOR_MEMORY"))
           .map(_.toLong)
           .getOrElse(
             Option(System.getenv("SPARK_MEM"))
               .map(_.toLong)
               .getOrElse(1024)
           )
       )
  }

  /**
   * Compare to stages, return true if the one which uses more partitions had a shorter stage time.
   * If both stages used the same number of partitions, return false
   * TODO: If historical runs are passed to the algorithm where the partitions remained the same, this will mean
   * TODO: we don't ever try to increase partitions
   */
  def morePartitionsIsBetter(first: StageInfo, second : StageInfo) : Boolean = {

    val lessPartitions :: morePartitions :: _ = List(first,second).sortBy(_.numPartitionsUsed)
    (morePartitions.executorCPUTime < lessPartitions.executorCPUTime) &&
      (morePartitions.numPartitionsUsed != lessPartitions.numPartitionsUsed)
  }

  def bestPartitionsSoFar(first: StageInfo, second : StageInfo) : Int = {
    List(first,second).sortBy(_.executorCPUTime).head.numPartitionsUsed
  }


  /**
   * Computes the amount of executor hours computed in the stage (stage time * num executors)  - the total time it took
   * to run all the tasks
   * WARNING: this metric will verry based on network/ connectivity issues, and if dynamic allocation is used or the
   * number of executors is not fixed between runs OR if there is traffic on the cluster
   * @param webUIInput
   * @return
   */
  def executorIdleTime(webUIInput: StageInfo): Int = {

    val executorStageTime = webUIInput.stageTime * webUIInput.numExectutors
    executorStageTime - webUIInput.totalTaskTime
  }

}

