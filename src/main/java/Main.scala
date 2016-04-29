import org.apache.spark.SparkContext

/**
  * Created by starqiu on 16-4-29.
  */
object Main {

  def main(args: Array[String]): Unit ={
    val sc = new SparkContext("spark://localhost:7077","Hello World")
    val file = sc.textFile("hdfs:///usr/hello")
  }
}

