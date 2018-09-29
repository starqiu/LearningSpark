import org.apache.spark.sql.SparkSession

/**
  * Created by qiuxing on 2017/11/30.
  */
object Test {

  def main(args: Array[String]): Unit = {
//    val v= if("1".equalsIgnoreCase(args.apply(0))){1}else{3}.+(3)
//    print(v)

    val sc = SparkSession.builder().master("local[2]").getOrCreate()

    val rdd =sc.sparkContext.parallelize(Array(Array(1,2),Array(3,4)))
    println(rdd.getClass)
//    rdd.map(x => x).foreach(println)
    rdd.flatMap(x=>x).foreach(println)
//    rdd.flatMap(_).foreach(println)

  }
}
