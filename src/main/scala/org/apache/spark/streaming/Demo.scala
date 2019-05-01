package org.apache.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Demo extends App with Logging {

  val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
  if (!log4jInitialized) {
    logInfo("Setting log level to [WARN]")
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(30))

  val timers: Map[String, LongAccumulator] =
    Seq("test-timer", "fm-timer", "m-timer", "rbk-timer")
      .map(t => t -> sc.longAccumulator(t))
      .toMap

  def meter[T](timerName: String)(block: => T): T = {
    val start = System.nanoTime()
    val result = block
    timers(timerName).add(System.nanoTime() - start)
    result
  }

  ssc
    .socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    .transform { rdd =>
      timers.values.foreach(_.reset())
      rdd
    }
    .transform { rdd =>
      meter("test-timer") {
        Thread.sleep(1)
      }
      rdd
    }
    .flatMap { x =>
      meter("fm-timer") {
        x.split(" ")
      }
    }
    .map { x =>
      meter("m-timer") {
        (x, 1)
      }
    }
    .reduceByKey {
      (a, b) =>
        meter("rbk-timer") {
          a + b
        }
    }
    .foreachRDD { r =>
      println(s"RDD size: ${r.count()}")
      println("------------------------------------------------------------")
      timers.values.foreach(println)
      println("------------------------------------------------------------")
    }

  ssc.start()
  ssc.awaitTermination()
}
