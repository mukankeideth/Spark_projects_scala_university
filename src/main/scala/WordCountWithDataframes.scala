import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountWithDataframes {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder
      .appName("Word Count with Dataframes")
      .config("spark.master", "local")
      .getOrCreate()

    import sparkSession.implicits._

    val lines = sparkSession
      .read
      .text("data/quijote.txt")
      .as[String]

    lines.printSchema()

    val words = lines.flatMap(_.split("\\s+"))

    val pairs = words.groupByKey(_.toLowerCase())

    val counts = pairs.count()

    counts.printSchema()

    counts
      .withColumnRenamed("count(1)", "count")
      .sort(desc("count"))
      .show()
  }
}

