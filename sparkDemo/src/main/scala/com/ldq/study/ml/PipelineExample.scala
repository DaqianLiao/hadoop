/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.ldq.study.ml

// $example on$
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
// $example off$
import org.apache.spark.sql.SparkSession

object PipelineExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("PipelineExample")
      .master("local[*]").getOrCreate()

    //训练集数据
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    //定义分词
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    //统计词频信息
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    //进行逻辑回归训练
    val lr = new LogisticRegression()
      //设置最大迭代次数
      .setMaxIter(10)
      //设置正则化参数
      .setRegParam(0.001)

    //定义模型步骤
    val stage = Array(tokenizer, hashingTF, lr)

    val pipeline = new Pipeline()
      .setStages(stage)

    // Fit the pipeline to training documents.
    val model = pipeline.fit(training)
    println(" print model stage ===========")
    model.stages.foreach(println)

    // Now we can optionally save the fitted pipeline to disk
    model.write.overwrite().save("data/model/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    pipeline.write.overwrite().save("data/model/unfit-lr-model")

    // And load it back in during production
    val sameModel = PipelineModel.load("data/model/spark-logistic-regression-model")


    // Prepare test documents, which are unlabeled (id, text) tuples.
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

// scalastyle:on println
