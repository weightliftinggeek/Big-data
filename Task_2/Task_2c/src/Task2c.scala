import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext, x: String, y: String) {
    // Load each line of the input data
    val twitterLines = sc.textFile("Assignment_Data/twitter.tsv")
    // Split each line of the input data into an array of strings
    val twitterdata = twitterLines.map(_.split("\t"))

    println("Months: x = " + x + ", y = " + y)

    // TODO: *** Put your solution here ***

val x = "200906"
val y = "200910"

   val matchmonthmap = twitterdata.map (r => (r(1), r(2).toInt, r(3)))
    val matchmonthx= matchmonthmap.filter(_._1 == x)
    val matchmonthy= matchmonthmap.filter(_._1 == y)

    val matchhashjoin = matchmonthx.map(r=>(r._3,r)).join(matchmonthy.map(r=>(r._3,r))).values
    matchhashjoin.collect()

    val matchnozeros = matchhashjoin.filter(r=>r._1._2 != 0 && r._2._2 != 0)

    val matchdifference = matchnozeros.map(r=> (r._1._3, r._1._2, r._2._2, r._2._2 - r._1._2))
    matchdifference.collect()

    val matchmaxfilter = matchdifference.reduce((x,y) => {if (x._4 > y._4) x else y})
    println("HashtagName: " + matchmaxfilter._1 + ", countX: " + matchmaxfilter._2 + ", countY: " + matchmaxfilter._3)

 }

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Check command line arguments
    if(args.length != 2) {
      println("Expected two command line arguments: <month x> and <month y>")
    }
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task2c")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 1)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext, args(0), args(1))
    // Stop Spark
    spark.stop()
  }
}

