import org.apache.spark.SparkContext

/**
  * Created by starqiu on 16-9-5.
  */
class PageRank {

}

object PageRank {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Page rank")
    val links = sc.parallelize(Array(("A", Array("D")), ("B", Array("A")), ("C", Array("A", "B")), ("D", Array("A", "C"))), 2)
    var ranks = sc.parallelize(Array(("A", 1.0), ("B", 1.0), ("C", 1.0), ("D", 1.0)), 2)

//    val maxIter: Int = 10
//    for (i <- (1 to 10)){
      val contribs = links.join(ranks, 2).flatMap{
        case (url,(links, rank)) => links.map(link => (link, rank/links.size))
      }

      ranks = contribs.reduceByKey(_ + _).mapValues(0.15+0.85*_)
      ranks.foreach(print _)

//    }

  }

}