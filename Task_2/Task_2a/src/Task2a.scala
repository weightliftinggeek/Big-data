import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext) {
    // Load each line of the input data
    val twitterLines = sc.textFile("Assignment_Data/twitter.tsv")
    // Split each line of the input data into an array of strings
    val twitterdata = twitterLines.map(_.split("\t"))


    // TODO: *** Put your solution here ***

    val highestcount = twitterdata.map(r=> (r(1),r(2).toInt, r(3))).sortBy(r=> r._2, false)
    val highestcount = twitterdata.map(r=> (r(1),r(2).toInt, r(3))).reduce((x,y) => {if (x._2 > y._2) x else y})
    println("month: " + highestcount._1 + ", count: " + highestcount._2 + ", hashtagName: " + highestcount._3)

    }
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task2a")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 1)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}


