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

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.DataFrame
// $example off$
import org.apache.spark.sql.SparkSession

object TfIdfExample {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("TfIdfExample")
      .master("local[*]").getOrCreate()

    // $example on$
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    def transfer(train: DataFrame): DataFrame = {
      val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
      val wordsData = tokenizer.transform(train)

      val hashingTF = new HashingTF()
        .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

      val hashData = hashingTF.transform(wordsData)
      // alternatively, CountVectorizer can also be used to get term frequency vectors
      hashData.show(false)
      val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
      val idfModel = idf.fit(hashData)
      val rescaledData = idfModel.transform(hashData)
      println("====default====")
      rescaledData.foreach(x => println(x))

      val idfMin = new IDF().setMinDocFreq(2).setInputCol("rawFeatures").setOutputCol("features")
      val idfMinModel = idfMin.fit(hashData)
      val rescaleMindData = idfMinModel.transform(hashData)
      println("==== setMinDocFreq = 2")
      rescaleMindData.foreach(x => println(x))
      rescaledData
    }

    val train = transfer(sentenceData)
    train.select("label", "features").show(false)
    // $example off$


    val testData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val test = transfer(testData)
    test.select("label", "features").show(false)

    spark.stop()
  }
}

// scalastyle:on println
