
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
    def groupbalancefunction(x:Int) : String = {
      if (x <= 500)
        return "Low"
      if (x >501 & x <=1500)
        return "Medium"
      else return "High"
    }

    val groupbalance = bankdata.map(r => groupbalancefunction(r(5).toInt))
    val groupbalancePairsRDD = groupbalance.map(groupbalance =>(groupbalance,1)).reduceByKey(_+_)
    groupbalancePairsRDD.collect()

    groupbalancePairsRDD.saveAsObjectFile("Assignment_Data/Task_1c-out")

    val loadedgroupbalancePairsRdd =
      sc.objectFile[(String,Int)]("Assignment_Data/Task_1c-out")

    loadedgroupbalancePairsRdd.saveAsTextFile("file:///root/labfiles/Labs/Assignment_2021/Assignment_Files/Task_1/Task_1c-out")

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1c")
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





