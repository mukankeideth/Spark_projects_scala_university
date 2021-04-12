import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AirportsInSpain {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder
      .appName("Airports in Spain")
      .config("spark.master", "local")
      .getOrCreate()

    val airportsDataFrame: DataFrame = sparkSession
      .read
      .format("csv")
      .options(Map("header" -> "true", "inferschema" -> "true"))
      .load("data/airports.csv")

    airportsDataFrame.printSchema()
    airportsDataFrame.show()

    airportsDataFrame
      .filter(airportsDataFrame("iso_region").contains("ES"))
      .groupBy("type")
      .count()
      .show(10)
  }

}
