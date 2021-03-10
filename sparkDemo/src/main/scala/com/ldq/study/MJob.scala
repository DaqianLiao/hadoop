package com.ldq.study

import java.util.UUID
import java.util.concurrent.Executors

import org.apache.spark.sql.SparkSession

/**
 * 同时运行多个job，并行提交
 */
//https://www.iteye.com/blog/qindongliang-2382800
object MJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MJob")
      .master("local[*]")
      .getOrCreate()

    val training = spark.createDataFrame(Seq(
      (1, 1, "hope you are well"),
      (2, 1, "nice to hear from you"),
      (3, 1, "happy weekend"),
      (4, 0, "fuck you"),
      (5, 0, "save money"),
      (6, 0, "stupid dog")
    )).toDF("userid", "label", "post")
    training.coalesce(1)

    training.cache()
    training.count()
    val jobExecutor = Executors.newFixedThreadPool(4)
    for( j <- Range(0, 2) ) {
      jobExecutor.execute(new Runnable {
        override def run(): Unit = {
          training.where(s"userid >= ${j}" ).show()
          val id = UUID.randomUUID().toString()
          training.coalesce(5).write.json(s"./data/mjob/${id}/")
        }
      })
    }
  }
}
