package com.ldq.study.base

import org.apache.spark.sql.SparkSession

import scala.util.Random


object CoreAndSql {
  val spark = SparkSession.builder
    .appName("Core and Sql example").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  def mean() {
    //开启隐式转换
    import spark.implicits._
    //初始化数据
    val data = for (i <- List.range(1, 10)) yield Random.nextInt(100)
    println(data)

    //rdd 方式获取平均值
    val rdd = sc.parallelize(data, 5)
    val mean = rdd.map(_.toDouble).reduce(_ + _) / rdd.count()
    println(mean)


    //使用sparkSQL编程实现
    val df = data.toDF("value")
    df.agg("value" -> "avg").show()


    //注册成临时表，使用SQL实现
    df.createOrReplaceGlobalTempView("df")
    spark.sql(" select avg(value) from global_temp.df").show()
  }

  def wordCount(): Unit = {
    //开启隐式转换
    import spark.implicits._

    //读取文件
    val path = "data/test/alt.atheism/53068"
    val words = sc.textFile(path)

    //分词 统计  rdd
    val cnt = words.flatMap(_.trim.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
    println(cnt.mkString(","))

    //使用sparkSQL统计
    val content = words.toDF("content")
    content.flatMap(x => x.mkString.trim.split(" ")).groupBy("value").count().show()
  }

  def topN() {
    //开启隐式转换
    import spark.implicits._

    val students = List(("Lily", 85, 90),
      ("jack", 90, 70),
      ("emily", 80, 99),
      ("leo", 100, 100))
    val n = 2

    //启动spark并行运行
    val rdd = spark.sparkContext.parallelize(students)
    //根据第三个字段进行倒序排列
    println(rdd.sortBy(_._3, ascending = false).take(n).mkString)

    val df = students.toDF("name", "math", "english")
    df.sort(df("math").desc).limit(n).show()

    //注册临时表
    df.registerTempTable("students")
    spark.sql("select * from students order by math desc limit 2").show()

    //注册全局临时视图，在global_temp库下面
    df.createOrReplaceGlobalTempView("st")
    spark.sql("select * from global_temp.st order by math desc limit 2").show()

  }


  def statatics(): Unit = {
    //开启隐式转换
    import spark.implicits._
    //初始化数据
    val data = for (i <- List.range(1, 10)) yield Random.nextInt(100)
    println(data)


    //rdd统计结果
    val rdd = sc.parallelize(data, 3)
    val max_value = rdd.reduce((a, b) => if (a > b) a else b)
    val min_value = rdd.reduce((a, b) => if (a < b) a else b)

    println(s"max value = $max_value, min_value = $min_value")

    //第二种统计方法
    val result = rdd.mapPartitions(iter => {
      var min_value = Integer.MAX_VALUE
      var max_value = Integer.MIN_VALUE
      for (x <- iter) {
        if (x > max_value) max_value = x
        if (x < min_value) min_value = x
      }
      Iterator((min_value, max_value))
    }).reduce((a, b) => {
      val min = if (a._1 <= b._1) a._1 else b._1
      val max = if (a._2 >= b._2) a._2 else b._2
      (min, max)
    })

    println(result)

    //将数据注册成dataframe 格式
    val df = data.toDF("value")

    //引入sql函数 max()  min()
    import org.apache.spark.sql.functions._
    df.agg(max("value") as "max_value", min("value") as "min_value").show()
  }

  def sortAndOrder(): Unit = {
    //开启隐式转换
    import spark.implicits._
    //初始化数据
    val data = for (i <- List.range(1, 10)) yield Random.nextInt(100)
    println(data)

    val df = data.toDF("value").sort("value")
    df.show
    import org.apache.spark.sql.expressions.Window
    val w = Window.orderBy("value")

    import org.apache.spark.sql.functions.row_number
    val result = df.withColumn("index", row_number().over(w) - 1)
    result.show()
  }

  def sortTwice(): Unit = {
    import spark.implicits._
    val students = List(("Lily", 85, 90),
      ("jack", 90, 70),
      ("emily", 80, 99),
      ("Emily", 80, 49),
      ("leo", 100, 100))

    val df = students.toDF("name", "math", "english")
    df.sort(df("math").desc, df("english").desc).show()
  }

  def main(args: Array[String]): Unit = {
    mean()
    wordCount()
    topN()
    statatics()
    sortAndOrder()
    sortTwice
    spark.close()
  }

}
