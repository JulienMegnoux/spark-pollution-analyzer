name := "Spark Pollution Analyzer"

version := "0.1"

scalaVersion := "3.7.3"  // correspond à celle installée chez toi

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1"
)

ThisBuild / organization := "com.julien.spark"
