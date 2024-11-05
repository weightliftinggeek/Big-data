import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)
case class VocabWord(vocabId: Int, word: String)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    // Read the input data
    val docwords = spark.read.schema(Encoders.product[Docword].schema).option("delimiter", " ").csv("Assignment_Data/docword.txt").as[Docword]
    val vocab = spark.read.schema(Encoders.product[VocabWord].schema).option("delimiter", " ").csv("Assignment_Data/vocab.txt").as[VocabWord]
    val docwordIndexFilename = "Assignment_Data/docword_index.parquet"
    // TODO: *** Put your solution here ***

    def getfirstletter (s: String):String = {
      s.substring(0,1)
    }

    val getfirstletterUdf =
      spark.udf.register[String, String]("getfirstletter", getfirstletter)


    val dc = docwords.toDF("docId", "vocab1", "count")
    val vc = vocab.toDF("vocab2", "word")

    val dcjoinvc = dc.join(vc, $"vocab1" === $"vocab2")

    val threefields = dcjoinvc.select($"word",$"docId",$"count")

    val fourfields = threefields.withColumn("firstLetter", getfirstletterUdf($"word"))

    fourfields.write.mode("overwrite").partitionBy("firstLetter").parquet(docwordIndexFilename)

    val fourfieldsParquetDF = spark.read.parquet(docwordIndexFilename)

    fourfieldsParquetDF.show(10)

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3b")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
