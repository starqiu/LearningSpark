import org.apache.spark.sql.{SparkSession, functions}

/**
  * Created by qiuxing on 2018/9/3.
  */


object TestWindowFunction {

  def main(args: Array[String]): Unit = {

//    this.getClass.getClassLoader.loadClass("org.apache.spark.sql")
//    functions.getClass.getMethods.map(m =>
//      (m.getName, m.getAnnotations.map(a => a.toString).mkString(","))).take(15).foreach(println(_))

    val spark = SparkSession
      .builder
      .appName(s"TestWindowFunction ")
        .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = List(
      ("站点1", "2017-01-01", 50),
      ("站点1", "2017-01-02", 45),
      ("站点1", "2017-01-03", 55),
      ("站点1", "2017-01-04", 55),
      ("站点2", "2017-01-01", 25),
      ("站点2", "2017-01-02", 29),
      ("站点2", "2017-01-03", 27)
    ).toDF("site", "date", "user_cnt")

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val wSpec = Window.partitionBy("site")
      .orderBy(desc("date"))
//        .rangeBetween(1L, 100L)
      .rowsBetween(-1, 1)

    df.withColumn("movingMin", min(df("user_cnt")).over(wSpec))
      .withColumn("movingMax", max(df("user_cnt")).over(wSpec))
      .withColumn("movingAvg", avg(df("user_cnt")).over(wSpec))
      .withColumn("movingStddev", stddev(df("user_cnt")).over(wSpec))
      .withColumn("movingSkewness", skewness(df("user_cnt")).over(wSpec))
      .withColumn("movingKurtosis", kurtosis(df("user_cnt")).over(wSpec))
      .withColumn("movingMedian", (expr("percentile_approx(user_cnt,0.5,10000000)")).over(wSpec))
      .show()

//    val df = Seq(
//        (1, "a")
////      , (1, "a")
//      , (2, "a")
//      , (3, "a")
//      , (4, "a")
//      , (1, "b")
//      , (2, "b")
//      , (3, "b"))
//      .toDF("id", "category")
//    df.withColumn("sum",
//      sum('id) over Window.partitionBy('category).orderBy('id).rangeBetween(-2,-1))
//      .show()
  }

}
