
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by qiuxing on 2018/7/11.
  */
class TestKudu {

}

object TestKudu{
  def main(args: Array[String]): Unit = {

    val sqlContext = SparkSession.builder().enableHiveSupport()
      .master("local").appName("test kudu").getOrCreate()


    import org.apache.kudu.spark.kudu._
    import org.apache.kudu.client._
    import collection.JavaConverters._
    import sqlContext.implicits._
    import org.apache.spark.sql.types._

    val schema = StructType(Array(StructField("REV",  IntegerType, false),
      StructField("REPORT_TIME",  StringType, false),
      StructField("VEHICLE_TAG",  IntegerType, false),
      StructField("LONGITUDE",  DoubleType, true),
      StructField("LATITUDE",  DoubleType, true),
      StructField("SPEED",  DoubleType, true),
      StructField("HEADING",  DoubleType, true),
      StructField("TRAIN_ASSIGNMENT",  IntegerType, true),
      StructField("PREDICTABLE",  IntegerType, true)
    ))

    val df = sqlContext.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header",true).schema(schema).load("D:\\software\\data\\avl2.csv")

    print(df.schema)
//    df.withColumn("REPORT_TIME", df("REPORT_TIME").getField())
//    println(df.count())
    val kuduContext = new KuduContext("master66:7051", sqlContext.sparkContext)

    // Create a new Kudu table from a dataframe schema
    // NB: No rows from the dataframe are inserted into the table
    kuduContext.createTable(
      "test_table", df.schema, Seq("REPORT_TIME", "VEHICLE_TAG"),
      new CreateTableOptions()
        .setNumReplicas(1)
        .addHashPartitions(List("REPORT_TIME", "VEHICLE_TAG").asJava, 3))
    kuduContext.insertRows(df, "test_table")
//    df.write.options(Map("kudu.master" -> "master66:7051", "kudu.table" -> "test_table")).mode("append").kudu
//    test(sqlContext)
  }

  private def test(sqlContext: SparkSession) = {
    import org.apache.kudu.spark.kudu._
    import org.apache.kudu.client._
    import collection.JavaConverters._
    import sqlContext.implicits._
    // Read a table from Kudu
    val df = sqlContext.read.options(Map("kudu.master" -> "master66:7051", "kudu.table" -> "kudu_table")).kudu

    // Query using the Spark API...
    df.select("id").filter($"id" >= 5).show()

    // ...or register a temporary table and use SQL
    df.registerTempTable("kudu_table")
    val filteredDF = sqlContext.sql("select id from kudu_table where id >= 5")

    filteredDF.show()
    // Use KuduContext to create, delete, or write to Kudu tables
    val kuduContext = new KuduContext("kudu.master:7051", sqlContext.sparkContext)

    // Create a new Kudu table from a dataframe schema
    // NB: No rows from the dataframe are inserted into the table
    kuduContext.createTable(
      "test_table", df.schema, Seq("REPORT_TIME"),
      new CreateTableOptions()
        .setNumReplicas(1)
        .addHashPartitions(List("REPORT_TIME").asJava, 3))

    // Insert data
    kuduContext.insertRows(df, "test_table")

    // Delete data
    kuduContext.deleteRows(filteredDF, "test_table")

    // Upsert data
    kuduContext.upsertRows(df, "test_table")

    // Update data
    val alteredDF = df.select($"id", $"count" + 1)
    kuduContext.updateRows(alteredDF, "test_table")

    // Data can also be inserted into the Kudu table using the data source, though the methods on KuduContext are preferred
    // NB: The default is to upsert rows; to perform standard inserts instead, set operation = insert in the options map
    // NB: Only mode Append is supported
    df.write.options(Map("kudu.master" -> "master66:7051", "kudu.table" -> "test_table")).mode("append").kudu

    // Check for the existence of a Kudu table
    kuduContext.tableExists("another_table")

    // Delete a Kudu table
    kuduContext.deleteTable("unwanted_table")
  }
}
