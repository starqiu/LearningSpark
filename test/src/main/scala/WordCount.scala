import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by qiuxing on 2017/11/21.
  */
object WordCount  {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test WordCount")
    val sc = new SparkContext(conf)
    val input = args.apply(0)
    val output = args.apply(1)
    println(input)
    sc.textFile(input)
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_ + _)
      .saveAsTextFile(output)

    sc.stop()
  }
}
