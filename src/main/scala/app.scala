// ============================================================================
// IMPORTS
// ============================================================================

import org.apache.spark.sql.{SparkSession, DataFrame}
// → Import des classes de base pour manipuler Spark et les DataFrames.

import org.apache.spark.sql.functions._
// → Import de toutes les fonctions SQL utiles (avg, max, col, when, etc.)

// IMPORTS POUR LA PARTIE 4  (GraphX = graphe de stations)

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// IMPORTS POUR LA PARTIE 5 (MLlib = Machine Learning)

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.{DecisionTreeRegressor, RandomForestRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator




// ============================================================================
// 1) NETTOYAGE — PM2.5 nettoyé et converti en DOUBLE
//    Objectif : partir d'un DataFrame brut et enlever le bruit (NA, doublons, etc.)
// ============================================================================

object Cleaning {

  // Fonction clean() : prend un DataFrame brut et retourne un DataFrame propre.
  // On utilise un implicit SparkSession pour avoir accès aux fonctions Spark.
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
      // - Remplace "NA" par une chaîne vide
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
// 2) FEATURES TEMPORELLES — Renommage et ajout de colonnes dérivées
//    Objectif : transformer les colonnes brutes en variables plus utiles
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
    // Ajout des colonnes temporelles :
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
// 3) STATISTIQUES SIMPLES — Moyennes PM25 par station, heure, mois, année
//    Objectif : avoir un premier aperçu descriptif de la pollution
// ============================================================================

object Stats {

  // Affiche plusieurs tableaux de stats de base (moyennes PM25 selon différents axes)
  def showBasic(df: DataFrame)(implicit spark: SparkSession): Unit = {

    println("\n=== Statistiques descriptives ===")

    // Moyenne PM25 par station
    df.groupBy("STATION")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy(desc("PM25_mean"))
      .show(20, truncate = false)

    // Moyenne PM25 par heure de la journée
    df.groupBy("hour")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy("hour")
      .show(24, truncate = false)

    // Moyenne PM25 par mois
    df.groupBy("month")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy("month")
      .show(12, truncate = false)

    // Moyenne PM25 par année
    df.groupBy("year")
      .agg(avg("PM25").alias("PM25_mean"))
      .orderBy("year")
      .show(truncate = false)
  }
}




// ============================================================================
// 4) ANALYSE APPROFONDIE — Stations, pics horaires, anomalies
//    Objectif : aller plus loin que les stats simples et analyser les profils
// ============================================================================

object DeepAnalysis {

  // ---------------------------------------------------------------------------
  // 3.1 Stations les plus exposées
  //  Moyenne et max de PM25 par station, triées par moyenne décroissante
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
  //  Identifier les heures et saisons où la pollution est la plus forte
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
  // 3.3 Indice global de pollution + détection d'anomalies
  //  Normaliser plusieurs polluants et construire un indice unique
  //  Détecter les points extrêmes (anomalies > moyenne + 3 * écart-type)
// ---------------------------------------------------------------------------
  def pollutionIndexAndAnomalies(df: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println("\n=== 3.3 Indice de pollution & anomalies ===")

    // Colonnes de pollution normalisées dans l'indice
    val cols = Seq("PM25","PM10","SO2","NO2","CO","O3")

    // Fonction qui retourne les min/max castés proprement pour une colonne donnée
    def minMax(c: String): (Double, Double) = {
      val r = df.agg(
        min(col(c).cast("double")).alias("min"),
        max(col(c).cast("double")).alias("max")
      ).first()

      val minv = r.getAs[Double]("min")
      val maxv = r.getAs[Double]("max")
      (minv, maxv)
    }

    // Normalisation colonne par colonne : (x - min) / (max - min)
    var dfN = df
    cols.foreach { c =>
      val (mn, mx) = minMax(c)
      dfN = dfN.withColumn(
        s"${c}_norm",
        when(col(c).isNull, 0.0)
          .otherwise( (col(c).cast("double") - mn) / (mx - mn + 1e-9) )
      )
    }

    // Indice global = moyenne des colonnes normalisées
    val pollutionIndex =
      cols.map(c => col(s"${c}_norm")).reduce(_ + _) / lit(cols.size)

    val dfI = dfN.withColumn("PollutionIndex", pollutionIndex)

    // Affichage des lignes les plus polluées (selon l’indice global)
    dfI
      .select("datetime","STATION","PM25","PM10","NO2","O3","PollutionIndex")
      .orderBy(desc("PollutionIndex"))
      .show(20,false)

    // Calcul moyenne + écart-type pour seuil d’anomalie
    val stats = dfI.agg(
      avg("PollutionIndex").alias("mean_idx"),
      stddev("PollutionIndex").alias("std_idx")
    ).first()

    val mean = stats.getAs[Double]("mean_idx")
    val sd   = stats.getAs[Double]("std_idx")
    val thr  = mean + 3 * sd // seuil d’anomalie (règle des 3 sigmas)

    val anomalies = dfI.filter(col("PollutionIndex") > thr)

    println(s"\n--- Anomalies detectees : ${anomalies.count()} ---")

    // Empêche Spark de tronquer les colonnes lors de l’affichage
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

  // Lance l’ensemble des analyses avancées
  def run(df: DataFrame)(implicit spark: SparkSession): Unit = {
    stationsMostExposed(df)
    peakHours(df)
    pollutionIndexAndAnomalies(df)
  }
}

// ============================================================================
// 5) PREDICTION — Spark MLlib (régression, arbre de décision, forêt aléatoire)
//    Objectif : prédire la valeur de PM2.5 à partir de variables explicatives
// ============================================================================

object Prediction {

  def run(dfFeat: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println("\n=== Partie 5 : Prediction de PM2.5 avec Spark MLlib ===")

    // 1) Données de base : label (PM25) + features (année, mois, station, etc.)
    val data = dfFeat
      .select(
        "PM25",          // variable à prédire (label)
        "year", "month", "day", "hour", "is_weekend",
        "STATION", "season",
        "datetime"
      )
      .na.drop()

    // 2) Encodage des variables catégorielles (STATION, season) en indices numériques
    val stationIndexer = new StringIndexer()
      .setInputCol("STATION")
      .setOutputCol("stationIndex")
      .setHandleInvalid("skip")

    val seasonIndexer = new StringIndexer()
      .setInputCol("season")
      .setOutputCol("seasonIndex")
      .setHandleInvalid("skip")

    // 3) Assemblage des features numériques + encodées en un vecteur "features"
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "year", "month", "day", "hour",
        "is_weekend",
        "stationIndex", "seasonIndex"
      ))
      .setOutputCol("features")

    // 4) Split train / test (80% apprentissage, 20% test)
    val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 42)

    // 5) Évaluateur commun (RMSE = Root Mean Squared Error)
    //  Mesure l'erreur moyenne entre PM25 réel et prédit
    val evaluator = new RegressionEvaluator()
      .setLabelCol("PM25")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // ========================================================================
    // Modèle 1 : Régression linéaire
    // ========================================================================
    val lr = new LinearRegression()
      .setLabelCol("PM25")
      .setFeaturesCol("features")
      .setMaxIter(20)

    // Pipeline = indexation station + saison + assemblage + modèle
    val lrPipeline = new Pipeline()
      .setStages(Array(stationIndexer, seasonIndexer, assembler, lr))

    val lrModel = lrPipeline.fit(trainData)
    val lrPred  = lrModel.transform(testData)

    val rmseLR = evaluator.evaluate(lrPred)
    println(f"\n=== Modele 1 : Regression lineaire ===")
    println(f"RMSE = $rmseLR%.2f")

    // ========================================================================
    // Modèle 2 : Arbre de décision
    // ========================================================================
    val dt = new DecisionTreeRegressor()
      .setLabelCol("PM25")
      .setFeaturesCol("features")

    val dtPipeline = new Pipeline()
      .setStages(Array(stationIndexer, seasonIndexer, assembler, dt))

    val dtModel = dtPipeline.fit(trainData)
    val dtPred  = dtModel.transform(testData)

    val rmseDT = evaluator.evaluate(dtPred)
    println(f"\n=== Modele 2 : Arbre de decision ===")
    println(f"RMSE = $rmseDT%.2f")

    // On affiche quelques exemples vrai vs prédit pour ce modèle (arbre de décision)
    println("\n--- Exemples (arbre de decision : PM25 vrai vs predit) ---")
    dtPred
      .select("datetime", "STATION", "PM25", "prediction")
      .orderBy(desc("datetime"))
      .show(20, truncate = false)

    // ========================================================================
    // Modèle 3 : Forêt aléatoire
    // ========================================================================
    val rf = new RandomForestRegressor()
      .setLabelCol("PM25")
      .setFeaturesCol("features")
      .setNumTrees(20)

    val rfPipeline = new Pipeline()
      .setStages(Array(stationIndexer, seasonIndexer, assembler, rf))

    val rfModel = rfPipeline.fit(trainData)
    val rfPred  = rfModel.transform(testData)

    val rmseRF = evaluator.evaluate(rfPred)
    println(f"\n=== Modele 3 : Foret aleatoire ===")
    println(f"RMSE = $rmseRF%.2f")
  }
}


// ============================================================================
// 6) TRAITEMENT EN TEMPS RÉEL (simulation de flux et détection d’anomalies)
//    Objectif : illustrer un flux "en temps réel" et détecter des dépassements
// ============================================================================

object RealTime {

  def run()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println("\n=== Partie 6 : Traitement en temps reel (simulation de flux) ===")

    // 1) Simulation d’un flux de données pour TOUTES les 12 stations
    // Chaque ligne simule une mesure PM25 à un instant donné
    val simulatedStream = Seq(
      ("2025-01-01 12:00", "Aotizhongxin",   80.0),
      ("2025-01-01 12:00", "Changping",      45.0),
      ("2025-01-01 12:00", "Dingling",       95.0),
      ("2025-01-01 12:00", "Dongsi",        180.0),  // anomalie
      ("2025-01-01 12:00", "Guanyuan",      160.0),  // anomalie
      ("2025-01-01 12:00", "Gucheng",       200.0),  // anomalie
      ("2025-01-01 12:00", "Huairou",        50.0),
      ("2025-01-01 12:00", "Nongzhanguan",   70.0),
      ("2025-01-01 12:00", "Shunyi",         60.0),
      ("2025-01-01 12:00", "Tiantan",        55.0),
      ("2025-01-01 12:00", "Wanliu",         40.0),
      ("2025-01-01 12:00", "Wanshouxigong", 170.0)   // anomalie
    ).toDF("timestamp", "station", "PM25")

    println("\n--- Flux simule recu  ---")
    simulatedStream.show(false)

    // 2) Transformation fonctionnelle : ajout d’un indicateur d’anomalie
  
    val transformed = simulatedStream.withColumn(
      "is_anomaly",
      col("PM25") > 150
    )

    println("\n--- Flux transforme (avec detection des anomalies) ---")
    transformed.show(false)

    // 3) Détection automatique des anomalies (filtre sur is_anomaly)
    val anomalies = transformed.filter(col("is_anomaly") === true)

    println("\n=== Anomalies detectees automatiquement ===")
    anomalies.show(false)
  }
}




// ============================================================================
// 7) MAIN — Chargement CSV, nettoyage, features, stats, analyses, ML, temps réel
// ============================================================================

object Main {

  // =======================
  // 4) Partie 4 GraphX : construction du graphe de stations
  // =======================

  // Sommets : (id, (station, avgPM25))
  // →On associe à chaque station un identifiant (VertexId) et sa PM25 moyenne
  def buildVertices(dfFeat: DataFrame)
                   (implicit spark: SparkSession): RDD[(VertexId, (String, Double))] = {

    val stationPollution = dfFeat
      .groupBy("STATION")
      .agg(avg("PM25").alias("avgPM25"))

    // zipWithIndex → assigne un ID unique à chaque ligne (station)
    stationPollution.rdd.zipWithIndex().map {
      case (row, id) =>
        val station = row.getString(0)   // STATION
        val pol     = row.getDouble(1)   // avgPM25
        (id, (station, pol))
    }
  }

  // Arêtes : connexions entre stations
  //  On définit à la main un "réseau" de stations reliées entre elles
  def buildEdges(dfFeat: DataFrame,
                 vertices: RDD[(VertexId, (String, Double))])
                (implicit spark: SparkSession): RDD[Edge[Double]] = {

    // Création d'une map station -> VertexId pour retrouver les IDs facilement
    val stationIdMap: Map[String, VertexId] =
      vertices.map { case (id, (st, _)) => (st, id) }.collect().toMap

    val bcMap = spark.sparkContext.broadcast(stationIdMap)

    // Connexions logiques entre stations (comme une petite "ligne de métro")
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

    // Construction des arêtes GraphX à partir des connexions station -> station
    spark.sparkContext.parallelize(connections)
      .flatMap { case (src, dst) =>
        val m = bcMap.value
        for {
          srcId <- m.get(src)
          dstId <- m.get(dst)
        } yield Edge(srcId, dstId, 1.0)  // poids = 1.0 (connexion neutre)
      }
  }

  // Fonction principale de la partie GraphX : construction + analyse du graphe
  def runGraphX(dfFeat: DataFrame)(implicit spark: SparkSession): Unit = {
    println("\n=== Partie 4 avec GraphX  ===")

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
    // 2) PageRank : importance dans le réseau
    //     Score d'importance de chaque station dans la structure du graphe
    // ============================

    val ranks = graph.pageRank(0.0001).vertices

    println("\n=== PageRank des stations (importance dans le reseau) ===")

    ranks.collect()
      .sortBy(-_._2)   
      .foreach {
        case (id, rank) =>
          val (st, _) = verticesMap.getOrElse(id, ("Inconnu", 0.0))
          println(s"Sommet $id ($st) : scorePageRank = $rank")
      }

    // ============================
    // 3) Propagation simplifiée de la pollution
    //    Chaque station envoie 10% de sa PM25 moyenne à ses voisines
    // ============================

    val propagated = graph.aggregateMessages[Double](
      triplet => {
        val pm25Src = triplet.srcAttr._2 
        triplet.sendToDst(pm25Src * 0.1) 
      },
      _ + _ 
    )

    println("\n=== Pollution transmise aux stations voisines (10% de la PM25 moyenne) ===")

    propagated.collect()
      .sortBy(-_._2)   
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

    // Création de la session Spark (point d'entrée)
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Beijing Pollution Analyzer")
      .master("local[*]") 
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("=== Chargement des fichiers PRSA ===")

    // Liste des 12 fichiers officiels PRSA (une station par fichier)
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

    // Lecture de tous les CSV en un seul DataFrame
    val dfRaw = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(files: _*)

    println(s"Nombre total de lignes chargees : ${dfRaw.count()}")

    // Étape 1 : nettoyage des données
    val dfClean = Cleaning.clean(dfRaw)
    println(s"Lignes apres nettoyage : ${dfClean.count()}")
    dfClean.select("YEAR","MONTH","DAY","HOUR","PM25_clean","STATION").show(10,false)

    // Étape 2 : ajout des features temporelles
    val dfFeat = Features.addFeatures(dfClean)
    dfFeat.select("datetime","season","is_weekend","hour_category","PM25","STATION")
      .show(10,false)

    // Étape 3 : statistiques descriptives
    Stats.showBasic(dfFeat)

    // Étape 4 : analyses avancées (stations, pics, anomalies)
    DeepAnalysis.run(dfFeat)

    // Étape 4 bis : modélisation du réseau avec GraphX
    runGraphX(dfFeat)

    // Étape 5 : prédiction de PM2.5 avec Spark MLlib
    Prediction.run(dfFeat)

    // Étape 6 : simulation de flux et détection d’anomalies en temps "réel"
    RealTime.run()

    // Fermeture propre de la session Spark
    spark.stop()
  }
}
