import com.beust.jcommander.{JCommander, Parameter}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by starqiu on 16-4-29.
  */
object App {

  def main(args: Array[String]): Unit ={

    val cmd: JCommander = new JCommander(AppArgs, args: _*)
    cmd.setProgramName("App", "Spark Learning App")
    if (AppArgs.help) {
      cmd.usage()
      return
    }
    //此处master使用spark://localhost:7077会报错，目前运行单机模式正常运行
    val conf = new SparkConf().setAppName("Hello World")
    if (AppArgs.debug) {
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)
    if (AppArgs.coreSitePath != null){
      sc.hadoopConfiguration.addResource(AppArgs.coreSitePath)
    }
    if (AppArgs.hdfsSitePath != null){
      sc.hadoopConfiguration.addResource(AppArgs.hdfsSitePath)
    }
    println(sc.hadoopConfiguration.toString)


    var hadoopConfiguration = new Configuration()
    hadoopConfiguration.addResource(AppArgs.hdfsSitePath)
    hadoopConfiguration.addResource(AppArgs.coreSitePath)


    println(hadoopConfiguration.toString)

//    val sc = new SparkContext("local")
//    val file = sc.textFile("hdfs://localhost:9000/usr/hello")
    testHelloWorld(sc)
//    val file = sc.textFile("/user/hello").cache()
//    file.foreach(println)
//    val blankLines = sc.longAccumulator
//    val t = file.map(line => {
//      if (StringUtils.isBlank(line.trim)){
//        blankLines.add(1)
//      }
//      line
//    })

//    t.top(1)
//    t.take(1)
////    println(t.reduce(_ + _))
//    println(blankLines.count)

  }



  def testHelloWorld(sc: SparkContext): Unit = {
    val file = sc.textFile("/user/hello")
    val lines = file.filter(s => s.contains("Hello World")).collect();
    lines.foreach(println)
  }

}
object AppArgs {
  @Parameter(names = Array("-h", "--help"), description = "打印此文档")
  var help: Boolean = false
  @Parameter(names = Array("-d", "--debug"), description = "调试模式")
  var debug: Boolean = false
  @Parameter(names = Array("-c", "--coreSitePath"), description = "hadoop core-site.xml的路径")
  var coreSitePath: String = null
  @Parameter(names = Array("-f", "--hdfsSitePath"), description = "hadoop hdfs-site.xml的路径")
  var hdfsSitePath: String = null
}
