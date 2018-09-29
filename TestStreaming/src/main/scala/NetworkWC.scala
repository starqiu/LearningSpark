import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
/**
  * Created by qiuxing on 2018/9/11.
  */
/**
  * @Author qiuxing
  * @Description //TODO $end$
  * @Date $time$ $date$
  * @Version 1.0
  **/
object NetworkWC {
  def main(args: Array[String]): Unit = {

//    testStreamingUsingNc(args)
    testStreamingUsingKafka(args)
  }

  private def testStreamingUsingNc(args: Array[String]) = {
    val hostname = args(0)
    val port = args(1).toInt

    LoggerLevels.setStreamingLogLevels()
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test streaming")
    //      SparkSession.builder().master("localhost").appName("test streaming").getOrCreate()

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("F:\\tmp")

    val lines = ssc.socketTextStream(hostname, port)
      .flatMap(_.split(" "))
      .map((_, 1))
      .checkpoint(Seconds(5))
      .updateStateByKey(updateFunc _, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    //      .reduceByKey(_ + _)

    lines.print()

    ssc.start()
    ssc.awaitTermination()


    println("end")
  }

  private def testStreamingUsingKafka(args: Array[String]) = {
    val Array(zkQuorum, group, topics, numThreads) = args

    LoggerLevels.setStreamingLogLevels()
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test streaming")
    //      SparkSession.builder().master("localhost").appName("test streaming").getOrCreate()

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("F:\\tmp")

    //Map类型存储的是   key： topic名字   values： 读取该topic的消费者的分区数
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //参数分别为StreamingContext,kafka的zk地址，消费者group，Map类型
    val kafkamessage = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    //_._2取出kafka的实际消息流
    val lines=kafkamessage.map(_._2)

    val wordCounts =lines.flatMap(_.split(" "))
      .map((_, 1))
      .checkpoint(Seconds(5))
      .updateStateByKey(updateFunc _, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1l))
//      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(5), 2)
    wordCounts.print(100)

//    lines.print()

    ssc.start()
    ssc.awaitTermination()


    println("end")
  }

  def updateFunc(iter: Iterator[(String,Seq[Int],Option[Int])]): Iterator[(String,Int)] ={
    iter.map {
      case (key, nums, sum) => {
        (key, nums.sum + sum.getOrElse(0))
      }
    }
  }

  def updateFunc2(nums:Seq[Int],sum: Option[Int]): Option[Int] ={
       Some(nums.sum + sum.getOrElse(0))
    }

}

object LoggerLevels  {

  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      println("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
