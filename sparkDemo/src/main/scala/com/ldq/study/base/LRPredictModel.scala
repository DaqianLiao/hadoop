package com.ldq.study.base

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Row, SparkSession}

object LRPredictModel {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]").getOrCreate()
    val model = PipelineModel
      .load("data/model/spark-logistic-regression-model")
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents.
    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
    // $example off$

    spark.stop()

  }

}
