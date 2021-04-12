import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AirportsSQL {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder
      .appName("Embedded SQL")
      .config("spark.master", "local")
      .getOrCreate()

    val dataFrame =
      sparkSession
        .read
        .format("csv")
        .options(Map("header" -> "true", "inferschema" -> "true"))
        .load("data/airports.csv")

    dataFrame.createOrReplaceTempView("airports")

    val sqlDataFrame = sparkSession.sql(
      "SELECT iso_country FROM airports WHERE iso_country == \"ES\""
    )
    sqlDataFrame.show()
  }

}
