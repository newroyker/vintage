package org.apache.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

object Demo extends App with Logging {

  val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
  if (!log4jInitialized) {
    logInfo("Setting log level to [WARN]")
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Demo")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  def meter[T](accum: LongAccumulator)(block: => T): T = {
    val start = System.nanoTime()
    val result = block
    accum.add(System.nanoTime() - start)
    accum.value
    result
  }


  val timer = ssc.sparkContext.longAccumulator("Timer")

  ssc
    .socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    .flatMap(_.split(" "))
    .map(x => (x, 1))
    .reduceByKey {
      (a, b) =>
        meter(timer) {
          a + b
        }
    }
    .print()

  ssc.start()
  ssc.awaitTermination()

}
