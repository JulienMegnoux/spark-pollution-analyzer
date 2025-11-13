import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Transformation {

  def process(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // ------------------------------------------------------------
    // 1️⃣ Conversion propre de la pollution en Double
    // ------------------------------------------------------------
    val dfCast = df.withColumn(
      "PollutionNum",
      regexp_replace(col("PollutionParticules"), ",", ".")
        .cast("double")
    )

    // ------------------------------------------------------------
    // 2️⃣ Extraction temporelle
    // ------------------------------------------------------------
    val dfTime = dfCast
      .withColumn("timestamp", current_timestamp())
      .withColumn("Hour", hour(col("timestamp")))
      .withColumn("Day", dayofmonth(col("timestamp")))
      .withColumn("Month", month(col("timestamp")))

    // ------------------------------------------------------------
    // 3️⃣ map / filter / flatMap via RDD (SANS CRASH)
    // ------------------------------------------------------------
    val rdd = dfTime
      .select("Station", "PollutionNum")
      .rdd

    val filtered = rdd.filter(row => {
      val value = row.getAs[Double]("PollutionNum")
      value != null && !value.isNaN
    })

    val mapped = filtered.map(row =>
      (row.getAs[String]("Station"), row.getAs[Double]("PollutionNum"))
    )

    val flatMapped = mapped.flatMap { case (station, pol) =>
      List((station, pol))
    }

    val rddDf = flatMapped.toDF("StationRDD", "PollutionRDD")

    // ------------------------------------------------------------
    // 4️⃣ Statistiques (Moyenne, Max, Min)
    // ------------------------------------------------------------
    val statsStation = dfTime.groupBy("Station").agg(
      avg("PollutionNum").alias("Moyenne_Station"),
      max("PollutionNum").alias("Max_Station"),
      min("PollutionNum").alias("Min_Station")
    )

    val statsLigne = dfTime.groupBy("Ligne").agg(
      avg("PollutionNum").alias("Moyenne_Ligne"),
      max("PollutionNum").alias("Max_Ligne"),
      min("PollutionNum").alias("Min_Ligne")
    )

    // ------------------------------------------------------------
    // 5️⃣ Fusion finale
    // ------------------------------------------------------------
    dfTime
      .join(statsStation, Seq("Station"), "left")
      .join(statsLigne, Seq("Ligne"), "left")
      .join(rddDf, dfTime("Station") === rddDf("StationRDD"), "left")
      .drop("StationRDD")
  }
}
