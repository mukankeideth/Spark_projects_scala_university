import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{min,max,avg,window}

object StreamingWindow {
  def streaming(path: String): Unit ={
    val spark = SparkSession
      .builder()
      .appName("StreamingWindow")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    val schema = StructType(
      List(
        StructField("poiID",StringType),
        StructField("nombre",StringType),
        StructField("direccion",StringType),
        StructField("telefono",StringType),
        StructField("correoelectronico",StringType),
        StructField("latitude",StringType),
        StructField("longitude",StringType),
        StructField("altitud",StringType),
        StructField("capacidad",StringType,nullable = true),
        StructField("capacidad_discapacitados",StringType),
        StructField("fechahora_ultima_actualizacion",StringType),
        StructField("libres",StringType, nullable = true),
        StructField("libres_discapacitados",StringType),
        StructField("nivelocupacion_naranja",StringType),
        StructField("nivelocupacion_rojo",StringType),
        StructField("smassa_sector_sare",StringType)

      )
    )
    var lines = spark
      .readStream
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .load(path)

    lines = lines.select($"nombre",window($"fechahora_ultima_actualizacion","8 minutes","2 minutes").alias("window"),
    (($"libres"/$"capacidad") * -1 + 1).alias("ocupacion")).groupBy($"window", $"nombre")
      .agg(avg($"ocupacion").alias("Ocupacion media"), max($"ocupacion").alias("Maxima ocupacion"), min($"ocupacion").alias("Minima ocupacion"))
      .orderBy($"window")



    val query = lines
      .writeStream
      .outputMode("complete")
      .format("console")
      .options( Map("numRows"->"1000","truncate"->"false"))
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      print("Please give a path argument")
    }
    else {
      streaming(args(0))
    }

  }
}
