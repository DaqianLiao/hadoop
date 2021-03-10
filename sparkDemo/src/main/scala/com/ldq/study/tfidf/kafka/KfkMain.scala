package com.ldq.study.tfidf.kafka

import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object KfkMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MJob")
      .master("local[*]")
      .getOrCreate()

    val kafkaProducer:Broadcast[KafkaSink[String,String]]={
      val kafkaConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers","localhost:9092")
        p.setProperty("key.serializer",classOf[StringSerializer].getName)
        p.setProperty("value.serializer",classOf[StringSerializer].getName)
        p
      }
      spark.sparkContext.broadcast(KafkaSink[String,String](kafkaConfig))
    }

    val training = spark.createDataFrame(Seq(
      (1, 1, "hope you are well"),
      (2, 1, "nice to hear from you"),
      (3, 1, "happy weekend"),
      (4, 0, "fuck you"),
      (5, 0, "save money"),
      (6, 0, "stupid dog")
    )).toDF("userid", "label", "post")

    val count = spark.sparkContext.longAccumulator("count")
    val topic = ""
    training.createOrReplaceGlobalTempView("train")
    val train = spark.sql("select  * from train")

    train.foreachPartition(rdd=>{
      rdd.foreach(record=>{
        kafkaProducer.value.send(topic,record.toString())
        count.add(1)
      })
    })

    println("count = " + count.value)
    spark.close()
  }

}
