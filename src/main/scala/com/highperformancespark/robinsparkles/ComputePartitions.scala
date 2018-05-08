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
case class WebUIInput(
  executorCPUTime: Int,
  stageTime: Int,
  totalInputSize : Double,
  numExectutors : Int,
  private val taskMetrics: List[Task]){


  val totalTaskTime: Int = taskMetrics.foldRight(0)((t, b) => b + t.totalTime)

  val numPartitionsUsed: Int = taskMetrics.length

}
object WebUIInput{
  def apply(s : StageVals, tList : Seq[TaskVals]) : WebUIInput = {

    val numExecutors = tList.map(_.executorId).distinct.size

    val inputSizeMb = s.bytesRead/(1024.0*1024.0)
    WebUIInput(
      s.executorCpuTime.toInt, //This is the amount of time that the executor was actually doing the computation for this stage
      s.stageDuration.toInt,
      inputSizeMb,
      numExecutors,
      tList.map(t => Task(t.duration.toInt)).toList)
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
  def fromStageMetric(previousRuns: List[WebUIInput]): Int = {
    val concurrentTasks = possibleConcurrentTasks()
    previousRuns match {
      case Nil => concurrentTasks
      case first :: Nil => (first.numPartitionsUsed + first.numPartitionsUsed)
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


  def fromStageMetricSharedCluster(previousRuns: List[WebUIInput]): Int = {
    val concurrentTasks = possibleConcurrentTasks()
    previousRuns match {
      case Nil => concurrentTasks
      case first :: Nil => (first.numPartitionsUsed + first.numExectutors)
      case first :: second :: _ =>
        val inputTaskSize = second.totalInputSize
        val taskMemoryMb = availableTaskMemoryMB()
        val floor: Int = Math.max(Math.round(inputTaskSize/taskMemoryMb).toInt, concurrentTasks)

        if(morePartitionsIsBetter(first,second)){
          //Increase the number of partitions
           Seq(floor, first.numPartitionsUsed, second.numPartitionsUsed).max + second.numExectutors
        }else{
          //If we overshot the number of partitions, use previous run if it was better
          List(first,second).sortBy(_.executorCPUTime).head.numPartitionsUsed
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
   * Compare to stages, return true if the one which uses more partitions had a shorter stage time
   */
  def morePartitionsIsBetter(first: WebUIInput, second : WebUIInput) : Boolean = {
    val morePartitions :: lessPartitions :: _ = List(first,second).sortBy(_.numPartitionsUsed)
    morePartitions.executorCPUTime > lessPartitions.executorCPUTime
  }

  def bestPartitionsSoFar(first: WebUIInput, second : WebUIInput) : Int = {
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
  def executorIdleTime(webUIInput: WebUIInput): Int = {

    val executorStageTime = webUIInput.stageTime * webUIInput.numExectutors
    executorStageTime - webUIInput.totalTaskTime
  }

}

