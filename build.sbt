ThisBuild / scalaVersion := "2.12.15"  // Databricks typically uses Scala 2.12
ThisBuild / organization := "com.marisync"

lazy val root = (project in file("."))
  .settings(
    name := "marisync",
    version := "0.1.0",
    
    libraryDependencies ++= Seq(
      // Spark Dependencies (use versions compatible with your Databricks runtime)
      "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-streaming" % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.4.1" % "provided",
      
      // Delta Lake
      "io.delta" %% "delta-core" % "2.4.0",
      
      // Kafka for streaming
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1" % "provided",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.15" % Test
    )
  ) 