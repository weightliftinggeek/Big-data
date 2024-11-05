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



    // TODO: *** Put your solution here ***

    val docwordsgrouppedDF = docwords.groupBy($"vocabId").sum("count")
    docwordsgrouppedDF.show

    val totalamountwordsDF = docwordsgrouppedDF.join(vocab, "vocabID").select($"word", $"sum(count)").orderBy($"word".asc,$"sum(count)".desc)
    totalamountwordsDF.show

    totalamountwordsDF.write.mode("overwrite").csv("Assignment_Data/Task_3a-out.csv")

    totalamountwordsDF.write.mode("overwrite").csv("file:///root/labfiles/Assignment_2021/Assignment_Files/Task_3/Task_3a-out.csv")

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3a")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
