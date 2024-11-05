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

    val matchmonthmap = twitterdata.map (r => (r(1), r(2).toInt, r(3)))

    def nextmonth (input:String) : String = {
      val y = input.substring(0,4)
      val m = input.substring(4,6)
      var output = 0
      if (m=="12") {output = ((y.toInt +1)*100)+1}
      else output = ((y.toInt)*100)+m.toInt+1
      output.toString
    }

    val matchmonthmap = twitterdata.map (r => (r(1), r(2).toInt, r(3)))

    val matchhashjoin = matchmonthmap.map(r=>((r._3, r._1),r)).join(matchmonthmap.map(r=>((r._3, nextmonth(r._1)),r))).values

    val matchdifference = matchhashjoin.map(r=> (r._1._3, r._1._2, r._2._2, r._1._2 - r._2._2, r._1._1, r._2._1))

    val matchmaxfilter = matchdifference.reduce((x,y) => {if (x._4 > y._4) x else y})
    println("Hash tag name: " + matchmaxfilter._1 + "\ncount of month " +  matchmaxfilter._6 + ": " + matchmaxfilter._3 + "\ncount of month " +  matchmaxfilter._5 + ": " + matchmaxfilter._2)

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("TaskBonus1")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 4)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}
