import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._



object CrimesBetweenYears {


  def main(args: Array[String]): Unit = {
      time(CrimeBetweenYears(args))
  }

  def time[R](block: => R):R = {
    val t0 = System.nanoTime()
    val result = block
    val  t1 = System.nanoTime()
    println("Elapsed time: " + (t1-t0)/1000000000 + " s")
    result
  }

  def CrimeBetweenYears(dates: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder
      .appName("Crimes last 24 months")
      .config("spark.master", "local")
      .getOrCreate()
    import sparkSession.sqlContext.implicits._
    val schema = StructType(
      List(
        StructField("ID", IntegerType, nullable = true),
        StructField("Case Number", StringType, nullable = true),
        StructField("Date", StringType, nullable = true),
        StructField("Block", StringType, nullable = true),
        StructField("IUCR", StringType, nullable = true),
        StructField("Primary Type", StringType, nullable = true),
        StructField("Description", StringType, nullable = true),
        StructField("Location Description", StringType, nullable = true),
        StructField("Arrest", BooleanType, nullable = true),
        StructField("Domestic", BooleanType, nullable = true),
        StructField("Beat", IntegerType, nullable = true),
        StructField("District", IntegerType, nullable = true),
        StructField("Ward", IntegerType, nullable = true),
        StructField("Community Area", IntegerType, nullable = true),
        StructField("FBI Code", StringType, nullable = true),
        StructField("X Coordinate", IntegerType, nullable = true),
        StructField("Y Coordinate", IntegerType, nullable = true),
        StructField("Year", IntegerType, nullable = true),
        StructField("Updated On", StringType, nullable = true),
        StructField("Latitude", DoubleType, nullable = true),
        StructField("Longitude", DoubleType, nullable = true),
        StructField("Location", StringType, nullable = true),
        StructField("Historical Wards 2003-2015", IntegerType, nullable = true),
        StructField("Zip Codes", IntegerType, nullable = true),
        StructField("Community Areas", IntegerType, nullable = true),
        StructField("Census Tracts", IntegerType, nullable = true),
        StructField("Wards", IntegerType, nullable = true),
        StructField("Boundaries - ZIP Codes", IntegerType, nullable = true),
        StructField("Police Districts", IntegerType, nullable = true),
        StructField("Police Beats", IntegerType, nullable = true)
      )
    )

    val crimesDataFrame: DataFrame = sparkSession
      .read
      .schema(schema)
      .format("csv")
      .options(Map("header" -> "true"))
      .load("data/Crimes_-_2001_to_present.csv")

    val updatedTFDf = crimesDataFrame.withColumn("Date", to_timestamp($"Date", "MM/dd/yyyy"))


    val out = updatedTFDf
      .filter(updatedTFDf("Date").geq(lit(dates(0))))
      .filter(updatedTFDf("Date").lt(lit(dates(1))))
      .groupBy("Year")
      .count()
      .orderBy($"count".desc)
    out.show(1)
    out.orderBy($"count").show(1)
    out.select(avg($"count")).show()
  }
}