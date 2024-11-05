import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    // Read the input data
    val docwords = spark.read.
      schema(Encoders.product[Docword].schema).
      option("delimiter", " ").
      csv("Assignment_Data/docword.txt").
      as[Docword]
    val frequentDocwordsFilename = "Assignment_Data/frequent_docwords.parquet"

    // TODO: *** Put your solution here ***

    val dc = docwords.toDF("docId", "vocab1", "count")

    val docwordsgrouppedDF = docwords.groupBy($"vocabId").sum("count")

    val dcfrequent = dc.join(docwordsgrouppedDF, $"vocab1" === $"vocabId").
      filter($"sum(count)" > 1000).
      select($"vocabId", $"docId", $"count")

    dcfrequent.write.mode("overwrite").parquet(frequentDocwordsFilename)

    dcfrequent.write.mode("overwrite").csv("Assignment_Data/Task_4a-out.csv")


  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task4a")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
