import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Main {
  def solution(spark: SparkSession, docIds: Seq[String]) {
    import spark.implicits._

    val docwordIndexFilename = "Assignment_Data/docword_index.parquet"
    val fourfieldsParquetDF = spark.read.parquet(docwordIndexFilename)
    val mostoccurencesword = fourfieldsParquetDF.select($"docId",$"word",$"count").orderBy($"count".desc).cache()


    println("Query words:")
    for(docId <- docIds) {
      println(docId.toInt)
      val mostoccurenceswordwhere = mostoccurencesword.where($"docId"===docId.toInt).limit(1)
      if (mostoccurenceswordwhere.count() != 0) {
        mostoccurenceswordwhere.show
      }
    }


    // TODO: *** Put your solution here ***

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3c")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark, args)
    // Stop Spark
    spark.stop()
  }
}
