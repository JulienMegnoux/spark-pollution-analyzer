# 🌍 Projet Spark — Analyse et Prévision de la Pollution Urbaine
Ce projet réalisé en Scala + Apache Spark analyse, modélise et prédit la pollution atmosphérique à partir de données massives provenant de différentes stations de mesure à Pékin.

Le projet inclut : nettoyage des données, statistiques avancées, détection d'anomalies, GraphX, MLlib et Structured Streaming.

------------------------------------------------------------
1) OBJECTIFS
------------------------------------------------------------
- Manipulation de big data avec Spark (RDD, DataFrames, SQL)
- Programmation fonctionnelle (map, filter, flatMap, reduce)
- Feature engineering (datetime, saison, week-end…)
- Analyse statistique avancée
- Détection automatique d'anomalies
- Modélisation GraphX (stations → graphe)
- Prédiction PM2.5 via MLlib
- Streaming en temps réel

------------------------------------------------------------
2) STRUCTURE DU CODE
------------------------------------------------------------
Le fichier app.scala contient :
- Cleaning : nettoyage, suppression doublons, cast des colonnes
- Features : datetime, saison, week-end, catégorie horaire
- Stats : moyennes PM2.5, variations temporelles
- DeepAnalysis : pics, anomalies, indice global pollution
- GraphX : PageRank, propagation de pollution
- Prediction : VectorAssembler, Régression linéaire, RMSE
- Streaming : flux simulé + détection anomalies
- Main : pipeline complet

------------------------------------------------------------
3) INSTALLATION DE SPARK
------------------------------------------------------------
Télécharger Spark : https://spark.apache.org/downloads  
Version recommandée : Spark 3.5.x, package Hadoop 3

Extraction :  
tar -xvf spark-3.5.0-bin-hadoop3.tgz

Ajouter Spark au PATH :  
export SPARK_HOME=~/spark-3.5.0-bin-hadoop3  
export PATH=$PATH:$SPARK_HOME/bin

Vérifier Spark :  
spark-shell --version

------------------------------------------------------------
4) LANCER LE PROJET
------------------------------------------------------------
Démarrer Spark dans le répertoire spark-pollution-analyser
spark-shell 

Charger le fichier Scala :  
:load src/main/scala/app.scala

Exécuter l’application :  
Main.main(Array())

------------------------------------------------------------
5) DONNÉES UTILISÉES
------------------------------------------------------------
Placer les fichiers PRSA dans un dossier data/ :

PRSA_Data_Aotizhongxin_20130301-20170228.csv  
PRSA_Data_Changping_20130301-20170228.csv  
PRSA_Data_Dingling_20130301-20170228.csv  
PRSA_Data_Dongsi_20130301-20170228.csv  
PRSA_Data_Guanyuan_20130301-20170228.csv  
PRSA_Data_Gucheng_20130301-20170228.csv  
PRSA_Data_Huairou_20130301-20170228.csv  
PRSA_Data_Nongzhanguan_20130301-20170228.csv  
PRSA_Data_Shunyi_20130301-20170228.csv  
PRSA_Data_Tiantan_20130301-20170228.csv  
PRSA_Data_Wanliu_20130301-20170228.csv  
PRSA_Data_Wanshouxigong_20130301-20170228.csv  

------------------------------------------------------------
6) RÉSULTATS OBTENUS
------------------------------------------------------------
- Statistiques PM2.5 (station, heure, mois, année)
- Classement des stations les plus exposées
- Pics horaires et saisonniers
- Indice global pollution
- Anomalies détectées automatiquement
- GraphX : graphe, PageRank, propagation
- Prédiction MLlib : régression linéaire, RMSE
- Streaming : anomalies détectées en temps réel

------------------------------------------------------------
7) RÉFÉRENCES
------------------------------------------------------------
Apache Spark : https://spark.apache.org/  
Spark: The Definitive Guide — O’Reilly  
Learning Spark — O’Reilly  
Scala : https://docs.scala-lang.org/

------------------------------------------------------------
8) AUTEURS
------------------------------------------------------------
Bissem MEDDOUR
Vivien LAMBERT
Roshanth RUBAN
Julien MEGNOUX
ADAM LOUFTI

Année : 2025
