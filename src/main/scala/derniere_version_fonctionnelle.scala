import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// ========================================================
// 1) Nettoyage — PM2.5 devient un vrai DOUBLE propre
// ========================================================
object Cleaning {

  def clean(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    df.dropDuplicates()
      .filter(col("YEAR").isNotNull)
      .filter(col("MONTH").isNotNull)
      .filter(col("DAY").isNotNull)
      .filter(col("HOUR").isNotNull)
      // 🔥 "NA" → null → cast double
      .withColumn("PM25_clean",
        regexp_replace(col("`PM2.5`"), "NA", "").cast("double")
      )
      // supprimer les lignes où PM25 reste null
      .filter(col("PM25_clean").isNotNull)
  }
}

// ========================================================
// 2) Features temporelles — on renomme PM25_clean en PM25
// ========================================================
object Features {

  def addFeatures(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val renamed = df
      .withColumnRenamed("YEAR",  "year")
      .withColumnRenamed("MONTH", "month")
      .withColumnRenamed("DAY",   "day")
      .withColumnRenamed("HOUR",  "hour")
      // 🔥 colonne déjà propre → renommée ici
      .withColumnRenamed("PM25_clean", "PM25")

    renamed
      .withColumn(
        "datetime",
        to_timestamp(
          concat_ws("-", $"year", $"month", $"day", $"hour"),
          "yyyy-M-d-H"
        )
      )
      .withColumn(
        "season",
        when($"month".isin(12,1,2), "winter")
          .when($"month".isin(3,4,5), "spring")
          .when($"month".isin(6,7,8), "summer")
          .otherwise("autumn")
      )
      .withColumn(
        "is_weekend",
        when(date_format($"datetime", "E").isin("Sat","Sun"), 1).otherwise(0)
      )
      .withColumn(
        "hour_category",
        when($"hour".between(0,5), "night")
          .when($"hour".between(6,11), "morning")
          .when($"hour".between(12,17), "afternoon")
          .otherwise("evening")
      )
  }
}

// ========================================================
// 3) Statistiques simples
// ========================================================
object Stats {

  def showBasic(df: DataFrame)(implicit spark: SparkSession): Unit = {

    println("\n=== Statistiques descriptives ===")

    df.groupBy("STATION")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy(desc("PM25_mean"))
      .show(20, truncate = false)

    df.groupBy("hour")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy("hour")
      .show(24, truncate = false)

    df.groupBy("month")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy("month")
      .show(12, truncate = false)

    df.groupBy("year")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy("year")
      .show(truncate = false)
  }
}

// ========================================================
// 4) Analyse approfondie — aucune modif nécessaire
// ========================================================
object DeepAnalysis {

  def stationsMostExposed(df: DataFrame)(implicit spark: SparkSession): Unit = {
    println("\n=== 3.1 Stations les plus exposées ===")

    df.groupBy("STATION")
      .agg(
        avg("PM25").alias("PM25_mean"),
        max("PM25").alias("PM25_max") // ← maintenant TOUJOURS une vraie valeur
      )
      .orderBy(desc("PM25_mean"))
      .show(20, false)
  }

  def peakHours(df: DataFrame)(implicit spark: SparkSession): Unit = {
    println("\n=== 3.2 Pics horaires ===")

    df.groupBy("hour")
      .agg(
        avg("PM25").alias("PM25_mean"),
        max("PM25").alias("PM25_max")
      )
      .orderBy(desc("PM25_mean"))
      .show(24, false)

    df.groupBy("season")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy(desc("PM25_mean"))
      .show(false)
  }

  def pollutionIndexAndAnomalies(df: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println("\n=== 3.3 Indice de pollution & anomalies ===")

    val cols = Seq("PM25","PM10","SO2","NO2","CO","O3")

    def minMax(c: String): (Double, Double) = {
      val r = df.agg(
        min(col(c).cast("double")).alias("min"),
        max(col(c).cast("double")).alias("max")
      ).first()

      val minv = r.getAs[Double]("min")
      val maxv = r.getAs[Double]("max")
      (minv, maxv)
    }

    var dfN = df
    cols.foreach { c =>
      val (mn, mx) = minMax(c)
      dfN = dfN.withColumn(
        s"${c}_norm",
        when(col(c).isNull, 0.0)
          .otherwise( (col(c).cast("double") - mn) / (mx - mn + 1e-9) )
      )
    }

    val pollutionIndex =
      cols.map(c => col(s"${c}_norm")).reduce(_ + _) / lit(cols.size)

    val dfI = dfN.withColumn("PollutionIndex", pollutionIndex)

    dfI
      .select("datetime","STATION","PM25","PM10","NO2","O3","PollutionIndex")
      .orderBy(desc("PollutionIndex"))
      .show(20,false)

    val stats = dfI.agg(
      avg("PollutionIndex").alias("mean_idx"),
      stddev("PollutionIndex").alias("std_idx")
    ).first()

    val mean = stats.getAs[Double]("mean_idx")
    val sd   = stats.getAs[Double]("std_idx")
    val thr  = mean + 3 * sd

    val anomalies = dfI.filter(col("PollutionIndex") > thr)

    println(s"\n--- Anomalies détectées : ${anomalies.count()} ---")
    anomalies.orderBy(desc("PollutionIndex")).show(20,false)
  }

  def run(df: DataFrame)(implicit spark: SparkSession): Unit = {
    stationsMostExposed(df)
    peakHours(df)
    pollutionIndexAndAnomalies(df)
  }
}

// ========================================================
// 5) MAIN — pas modifié
// ========================================================
object Main {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Beijing Pollution Analyzer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("=== Chargement des fichiers PRSA ===")

    val files = Seq(
      "data/PRSA_Data_Aotizhongxin_20130301-20170228.csv",
      "data/PRSA_Data_Changping_20130301-20170228.csv",
      "data/PRSA_Data_Dingling_20130301-20170228.csv",
      "data/PRSA_Data_Dongsi_20130301-20170228.csv",
      "data/PRSA_Data_Guanyuan_20130301-20170228.csv",
      "data/PRSA_Data_Gucheng_20130301-20170228.csv",
      "data/PRSA_Data_Huairou_20130301-20170228.csv",
      "data/PRSA_Data_Nongzhanguan_20130301-20170228.csv",
      "data/PRSA_Data_Shunyi_20130301-20170228.csv",
      "data/PRSA_Data_Tiantan_20130301-20170228.csv",
      "data/PRSA_Data_Wanliu_20130301-20170228.csv",
      "data/PRSA_Data_Wanshouxigong_20130301-20170228.csv"
    )

    val dfRaw = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(files: _*)

    println(s"Nombre total de lignes chargées : ${dfRaw.count()}")

    val dfClean = Cleaning.clean(dfRaw)
    println(s"Lignes après nettoyage : ${dfClean.count()}")
    dfClean.select("YEAR","MONTH","DAY","HOUR","PM25_clean","STATION").show(10,false)

    val dfFeat = Features.addFeatures(dfClean)
    dfFeat.select("datetime","season","is_weekend","hour_category","PM25","STATION")
      .show(10,false)

    Stats.showBasic(dfFeat)

    DeepAnalysis.run(dfFeat)

    spark.stop()
  }
}
