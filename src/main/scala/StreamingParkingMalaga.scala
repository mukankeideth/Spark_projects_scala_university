import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}


object StreamingParkingMalaga {
  def streaming(path: String): Unit ={
    val spark = SparkSession
      .builder()
      .appName("StreamingParkingMalaga")
      .config("spark.master", "local")
      .getOrCreate()

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

    lines = lines.select("nombre","fechahora_ultima_actualizacion","capacidad","libres")



    val query = lines
    .writeStream
    .outputMode("update")
    .format("console")
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
