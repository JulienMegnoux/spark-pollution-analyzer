// ============================================================================
// IMPORTS
// ============================================================================

import org.apache.spark.sql.{SparkSession, DataFrame}
// → Import des classes de base pour manipuler Spark et les DataFrames.

import org.apache.spark.sql.functions._
// → Import de toutes les fonctions SQL utiles (avg, max, col, when, etc.)

// IMPORTS POUR LA PARIE 4 

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD



// ============================================================================
// 1) NETTOYAGE — PM2.5 nettoye et converti en DOUBLE
// ============================================================================

object Cleaning {

  // Fonction clean() : prend un DataFrame brut et retourne un DataFrame propre.
  def clean(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    df
      .dropDuplicates()
      // → Supprime les lignes strictement identiques.

      .filter(col("YEAR").isNotNull)
      .filter(col("MONTH").isNotNull)
      .filter(col("DAY").isNotNull)
      .filter(col("HOUR").isNotNull)
      // → On supprime les lignes où les informations temporelles sont manquantes.

      // -----------------------------------------------------------------------
      // Nettoyage de PM2.5 :
      // - Remplace "NA" par une chaine vide
      // - Convertit ensuite en double → PM25_clean
      // -----------------------------------------------------------------------
      .withColumn(
        "PM25_clean",
        regexp_replace(col("`PM2.5`"), "NA", "").cast("double")
      )

      // → Elimination des lignes où PM25_clean reste null (valeurs inutilisables).
      .filter(col("PM25_clean").isNotNull)
  }
}




// ============================================================================
// 2) FEATURES TEMPORELLES — Renommage et ajout de colonnes derivees
// ============================================================================

object Features {

  def addFeatures(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Correction d’un changement de comportement Spark sur le parsing des dates.
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    // -------------------------------------------------------------------------
    // Renommage des colonnes : YEAR → year, etc.
    // PM25_clean devient PM25 (colonne finale propre)
    // -------------------------------------------------------------------------
    val renamed = df
      .withColumnRenamed("YEAR",  "year")
      .withColumnRenamed("MONTH", "month")
      .withColumnRenamed("DAY",   "day")
      .withColumnRenamed("HOUR",  "hour")
      .withColumnRenamed("PM25_clean", "PM25")

    // -------------------------------------------------------------------------
    // Ajout des colonnes temporelles
    // - datetime : vraie colonne timestamp
    // - season : saison correspondant au mois
    // - is_weekend : 1 si samedi/dimanche, 0 sinon
    // - hour_category : catégorie d'heure (night/morning/afternoon/evening)
    // -------------------------------------------------------------------------
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




// ============================================================================
// 3) STATISTIQUES SIMPLES — Moyennes PM25 par station, heure, mois, annee
// ============================================================================

object Stats {

  def showBasic(df: DataFrame)(implicit spark: SparkSession): Unit = {

    println("\n=== Statistiques descriptives ===")

    // Moyenne PM25 par station
    df.groupBy("STATION")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy(desc("PM25_mean"))
      .show(20, truncate = false)

    // Moyenne PM25 par heure de la journee
    df.groupBy("hour")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy("hour")
      .show(24, truncate = false)

    // Moyenne PM25 par mois
    df.groupBy("month")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy("month")
      .show(12, truncate = false)

    // Moyenne PM25 par annee
    df.groupBy("year")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy("year")
      .show(truncate = false)
  }
}




// ============================================================================
// 4) ANALYSE APPROFONDIE — Stations, pics horaires, anomalies
// ============================================================================

object DeepAnalysis {

  // ---------------------------------------------------------------------------
  // 3.1 Stations les plus exposees
  // ---------------------------------------------------------------------------
  def stationsMostExposed(df: DataFrame)(implicit spark: SparkSession): Unit = {
    println("\n=== 3.1 Stations les plus exposees ===")

    df.groupBy("STATION")
      .agg(
        avg("PM25").alias("PM25_mean"),
        max("PM25").alias("PM25_max") // toujours une vraie valeur → PM25 est propre
      )
      .orderBy(desc("PM25_mean"))
      .show(20, false)
  }

  // ---------------------------------------------------------------------------
  // 3.2 Pics horaires et saisons critiques
  // ---------------------------------------------------------------------------
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

  // ---------------------------------------------------------------------------
  // 3.3 Indice global de pollution + detection d'anomalies
  // ---------------------------------------------------------------------------
  def pollutionIndexAndAnomalies(df: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println("\n=== 3.3 Indice de pollution & anomalies ===")

    // Colonnes de pollution normalisees dans l'indice
    val cols = Seq("PM25","PM10","SO2","NO2","CO","O3")

    // Fonction qui retourne les min/max castes proprement
    def minMax(c: String): (Double, Double) = {
      val r = df.agg(
        min(col(c).cast("double")).alias("min"),
        max(col(c).cast("double")).alias("max")
      ).first()

      val minv = r.getAs[Double]("min")
      val maxv = r.getAs[Double]("max")
      (minv, maxv)
    }

    // Normalisation colonne par colonne
    var dfN = df
    cols.foreach { c =>
      val (mn, mx) = minMax(c)
      dfN = dfN.withColumn(
        s"${c}_norm",
        when(col(c).isNull, 0.0)
          .otherwise( (col(c).cast("double") - mn) / (mx - mn + 1e-9) )
      )
    }

    // Indice global = moyenne des colonnes normalisees
    val pollutionIndex =
      cols.map(c => col(s"${c}_norm")).reduce(_ + _) / lit(cols.size)

    val dfI = dfN.withColumn("PollutionIndex", pollutionIndex)

    // Affichage des lignes les plus polluees
    dfI
      .select("datetime","STATION","PM25","PM10","NO2","O3","PollutionIndex")
      .orderBy(desc("PollutionIndex"))
      .show(20,false)

    // Calcul moyenne + ecart-type pour seuil d’anomalie
    val stats = dfI.agg(
      avg("PollutionIndex").alias("mean_idx"),
      stddev("PollutionIndex").alias("std_idx")
    ).first()

    val mean = stats.getAs[Double]("mean_idx")
    val sd   = stats.getAs[Double]("std_idx")
    val thr  = mean + 3 * sd // seuil d’anomalie

    val anomalies = dfI.filter(col("PollutionIndex") > thr)

    println(s"\n--- Anomalies detectees : ${anomalies.count()} ---")

    // Empêche Spark de tronquer les colonnes
    spark.conf.set("spark.sql.debug.maxToStringFields", 200)

    println("\n=== TOP anomalies (trie par indice de pollution) ===")

    anomalies
      .select(
        col("datetime"),
        col("STATION"),
        col("PM25"),
        col("PM10"),
        col("NO2"),
        col("O3"),
        col("PollutionIndex")
      )
      .orderBy(desc("PollutionIndex"))
      .show(50, truncate = false)

  }

  // Lance l’ensemble des analyses
  def run(df: DataFrame)(implicit spark: SparkSession): Unit = {
    stationsMostExposed(df)
    peakHours(df)
    pollutionIndexAndAnomalies(df)
  }
}




// ============================================================================
// 5) MAIN — Chargement CSV, nettoyage, features, stats, analyses
// ============================================================================

object Main {

  // =======================
  // 4) Partie 4 GraphX
  // =======================

  // Sommets : (id, (station, avgPM25))
  def buildVertices(dfFeat: DataFrame)
                   (implicit spark: SparkSession): RDD[(VertexId, (String, Double))] = {

    
    val stationPollution = dfFeat
      .groupBy("STATION")
      .agg(avg("PM25").alias("avgPM25"))

    stationPollution.rdd.zipWithIndex().map {
      case (row, id) =>
        val station = row.getString(0)   // STATION
        val pol     = row.getDouble(1)   // avgPM25
        (id, (station, pol))
    }
  }

  // Arêtes : connexions entre stations
  def buildEdges(dfFeat: DataFrame,
                 vertices: RDD[(VertexId, (String, Double))])
                (implicit spark: SparkSession): RDD[Edge[Double]] = {

    val stationIdMap: Map[String, VertexId] =
      vertices.map { case (id, (st, _)) => (st, id) }.collect().toMap

    val bcMap = spark.sparkContext.broadcast(stationIdMap)

    val connections = Seq(
      ("Aotizhongxin", "Dongsi"),
      ("Dongsi", "Guanyuan"),
      ("Guanyuan", "Wanshouxigong"),
      ("Wanshouxigong", "Tiantan"),
      ("Tiantan", "Wanliu"),
      ("Wanliu", "Gucheng"),
      ("Gucheng", "Nongzhanguan"),
      ("Nongzhanguan", "Shunyi"),
      ("Shunyi", "Huairou"),
      ("Huairou", "Changping"),
      ("Changping", "Dingling")
    )

    spark.sparkContext.parallelize(connections)
      .flatMap { case (src, dst) =>
        val m = bcMap.value
        for {
          srcId <- m.get(src)
          dstId <- m.get(dst)
        } yield Edge(srcId, dstId, 1.0)
      }
  }

  def runGraphX(dfFeat: DataFrame)(implicit spark: SparkSession): Unit = {
    println("\n=== Partie GraphX ===")

    // Construction du graphe
    val vertices = buildVertices(dfFeat)
    val edges    = buildEdges(dfFeat, vertices)
    val graph    = Graph(vertices, edges, ("Unknown", 0.0))

    // Petit cache local pour retrouver le nom de station à partir de l'id
    val verticesMap: Map[VertexId, (String, Double)] = vertices.collect().toMap

    // ============================
    // 1) Affichage sommets / arêtes
    // ============================

    println("\n=== Sommets (stations) ===")
    graph.vertices.collect().foreach {
      case (id, (st, avg)) =>
        println(s"Sommet $id : station=$st, avgPM25=$avg")
    }

    println("\n=== Aretes (connexions) ===")
    graph.edges.collect().foreach { e =>
      println(s"Arete ${e.srcId} -> ${e.dstId}, poids=${e.attr}")
    }

    println("\n=== Etude de la propagation de la pollution a travers le reseau ===")

    // ============================
    // 2) PageRank : importance dans le reseau
    // ============================

    val ranks = graph.pageRank(0.0001).vertices

    println("\n=== PageRank des stations (importance dans le reseau) ===")

    ranks.collect()
      .sortBy(-_._2)   // tri décroissant sur le score
      .foreach {
        case (id, rank) =>
          val (st, _) = verticesMap.getOrElse(id, ("Inconnu", 0.0))
          println(s"Sommet $id ($st) : scorePageRank = $rank")
      }

    // ============================
    // 3) Propagation simplifiée de la pollution
    // ============================

    val propagated = graph.aggregateMessages[Double](
      triplet => {
        val pm25Src = triplet.srcAttr._2 // moyenne PM25 du sommet source
        triplet.sendToDst(pm25Src * 0.1) // 10% envoyé au voisin
      },
      _ + _ // somme des contributions reçues
    )

    println("\n=== Pollution transmise aux stations voisines (10% de la PM25 moyenne) ===")

    propagated.collect()
      .sortBy(-_._2)   // tri décroissant sur la pollution reçue
      .foreach {
        case (id, value) =>
          val (st, _) = verticesMap.getOrElse(id, ("Inconnu", 0.0))
          println(s"Sommet $id ($st) recoit  $value unites de pollution")
      }
  } 

  // =======================
  // MAIN
  // =======================

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Beijing Pollution Analyzer")
      .master("local[*]") // execution locale multi-thread
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

    println(s"Nombre total de lignes chargees : ${dfRaw.count()}")

    val dfClean = Cleaning.clean(dfRaw)
    println(s"Lignes apres nettoyage : ${dfClean.count()}")
    dfClean.select("YEAR","MONTH","DAY","HOUR","PM25_clean","STATION").show(10,false)

    val dfFeat = Features.addFeatures(dfClean)
    dfFeat.select("datetime","season","is_weekend","hour_category","PM25","STATION")
      .show(10,false)

    Stats.showBasic(dfFeat)
    DeepAnalysis.run(dfFeat)

    // Partie 4 : GraphX
    runGraphX(dfFeat)

    spark.stop()
  }
}


