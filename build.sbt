ThisBuild / scalaVersion := "2.12.15"  // Databricks typically uses Scala 2.12
ThisBuild / organization := "com.marisync"

// Add these settings
ThisBuild / javacOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "marisync",
    version := "0.1.0",
    
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    
    // Add fork options for tests
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    
    libraryDependencies ++= Seq(
      // Spark Dependencies - provided for main, but needed for test
      "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-streaming" % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.4.1" % "provided",
      
      // Also add Spark dependencies for test scope
      "org.apache.spark" %% "spark-sql" % "3.4.1" % "test",
      "org.apache.spark" %% "spark-streaming" % "3.4.1" % "test",
      "org.apache.spark" %% "spark-mllib" % "3.4.1" % "test",
      
      // Delta Lake
      "io.delta" %% "delta-core" % "2.4.0" % "provided",
      "io.delta" %% "delta-core" % "2.4.0" % "test",
      
      // Kafka
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1" % "provided",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "org.apache.kafka" % "kafka-clients" % "3.4.1" % "test",
      "org.mockito" %% "mockito-scala" % "1.17.12" % Test,
      "org.mockito" %% "mockito-scala-scalatest" % "1.17.12" % Test,
      
      // HTTP and JSON dependencies (needed at runtime)
      "org.scalaj" %% "scalaj-http" % "2.4.2",
      "org.json4s" %% "json4s-native" % "3.7.0-M11",
      "org.json4s" %% "json4s-core" % "3.7.0-M11",
      "org.json4s" %% "json4s-ast" % "3.7.0-M11",
      "org.json4s" %% "json4s-jackson" % "3.7.0-M11",
      
      // Also add json4s for test scope
      "org.json4s" %% "json4s-native" % "3.7.0-M11" % "test",
      "org.json4s" %% "json4s-core" % "3.7.0-M11" % "test",
      "org.json4s" %% "json4s-ast" % "3.7.0-M11" % "test",
      "org.json4s" %% "json4s-jackson" % "3.7.0-M11" % "test",
      
      "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
    )
  ) 