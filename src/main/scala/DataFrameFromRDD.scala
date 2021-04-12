import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object DataFrameFromRDD {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val students = List(("Luis", 23), ("Ana", 24), ("Jose", 20), ("Carlos", 26), ("Maria", 23))

    val sparkSession = SparkSession
      .builder
      .appName("Create DataFrame from RDD")
      .config("spark.master", "local")
      .getOrCreate()

    import sparkSession.implicits._

    val studenstRDD = sparkSession.sparkContext.parallelize(students)

    val studentsDF = studenstRDD.toDF("student", "age")

    studentsDF.printSchema()
    studentsDF.show()
  }
}
