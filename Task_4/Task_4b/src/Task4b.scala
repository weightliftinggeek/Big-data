import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class VocabWord(vocabId: Int, word: String)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    // Read the input data
    val vocab = spark.read.
      schema(Encoders.product[VocabWord].schema).
      option("delimiter", " ").
      csv("Assignment_Data/vocab.txt").
      as[VocabWord]
    val frequentDocwordsFilename = "Assignment_Data/frequent_docwords.parquet"

    // TODO: *** Put your solution here ***
    val dcfrequentparquetDF = spark.read.parquet(frequentDocwordsFilename)

    val dcfrequentparquetDFvocab = dcfrequentparquetDF.join(vocab, "vocabId")

    val dcfrequentparquetDF_right = dcfrequentparquetDFvocab.toDF("vocabId1", "docId1", "count1", "word1")

    val dcfrequentjoin = dcfrequentparquetDFvocab.join(dcfrequentparquetDF_right, dcfrequentparquetDF("docId") === dcfrequentparquetDF_right("docId1"))

    val dcfrequentjoinselected = dcfrequentjoin.select ($"word", $"word1", $"docId").dropDuplicates("word", "word1", "docId")

    val dcfrequentjoincounted = dcfrequentjoinselected.groupBy("word","word1","docId").count()

    val dcfrequentjoinsum = dcfrequentjoincounted.groupBy("word", "word1").sum("count")

    val dcfrequentDF = dcfrequentjoinsum.filter(dcfrequentjoinsum("word")>dcfrequentjoinsum("word1")).orderBy(desc("sum(count)"))
    dcfrequentDF.show
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task4b")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
