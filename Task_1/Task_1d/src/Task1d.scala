
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext) {
    // Load each line of the input data
    val bankdataLines = sc.textFile("Assignment_Data/bank.csv")
    // Split each line of the input data into an array of strings
    val bankdata = bankdataLines.map(_.split(";"))

    // TODO: *** Put your solution here ***
    val banksplitselected = bankdata.map(r=> (r(3),r(5).toInt, r(1), r(2), r(7)))

    val banksplitselectedsort1 = banksplitselected.sortBy(r=>(r._1, -r._2))

    banksplitselectedsort1.saveAsObjectFile("Assignment_Data/Task_1d-out")

    val loadedbanksplitselectedsorted1 =
      sc.objectFile[(String, Int, String, String, String)]("Assignment_Data/Task_1d-out")

    loadedbanksplitselectedsorted1.saveAsTextFile("file:///root/labfiles/Labs/Assignment_2021/Assignment_Files/Task_1/Task_1d-out")


  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1d")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}

