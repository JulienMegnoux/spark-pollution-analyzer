import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main {

  def pretty(df: DataFrame, n: Int = 10): Unit =
    df.show(n, truncate = 60)

  // Sélection de colonnes propres pour affichage
  def compact(df: DataFrame): DataFrame =
    df.select(
      "station",
      "PM25",
      "PM10",
      "SO2",
      "NO2",
      "CO",
      "O3",
      "year",
      "month",
      "day",
      "hour"
    )

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Beijing Pollution Analyzer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.hadoop.io.nativeio.enabled", "false")

    println("=== Chargement des fichiers PRSA ===")

    // Liste des fichiers CSV
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

    // 🔧 NORMALISATION DES COLONNES
    val dfRenamed = dfRaw.columns.foldLeft(dfRaw) { (df, colName) =>
      df.withColumnRenamed(colName, colName.trim.replace(" ", "").replace(".", "").toUpperCase)
    }

    // PM2.5 → PM25
    val dfFixed = dfRenamed.withColumnRenamed("PM25", "PM25")

    val dfNoDup = dfFixed.dropDuplicates()

    println(s"Lignes après suppression des doublons : ${dfNoDup.count()}")

    // Nettoyage des valeurs manquantes
    val dfClean = dfNoDup
      .filter($"YEAR".isNotNull)
      .filter($"MONTH".isNotNull)
      .filter($"DAY".isNotNull)
      .filter($"HOUR".isNotNull)
      .filter($"PM25".isNotNull)

    println(s"Lignes après nettoyage : ${dfClean.count()}")
    println("=== Aperçu des données nettoyées ===")

    pretty(
      dfClean
        .withColumnRenamed("PM25", "PM25") // pour compact()
        .withColumnRenamed("YEAR", "year")
        .withColumnRenamed("MONTH", "month")
        .withColumnRenamed("DAY", "day")
        .withColumnRenamed("HOUR", "hour"),
      10
    )

    spark.stop()
  }
}
