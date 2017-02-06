import org.apache.spark.SparkContext

/**
  * Created by starqiu on 16-4-29.
  */
object Main {

  def main(args: Array[String]): Unit ={
    //此处master使用spark://localhost:7077会报错，目前运行单机模式正常运行
    val sc = new SparkContext("local","Hello World")
//    val file = sc.textFile("hdfs://localhost:9000/usr/hello")
    val file = sc.textFile("hdfs:///usr/hello")
    val lines = file.filter(s => s.contains("Hello World")).collect();
    lines.foreach(println)
  }


}

