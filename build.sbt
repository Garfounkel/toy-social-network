name := "Spark Project"

version := "1.0"

scalaVersion := "2.11.8"
val kafkaVersion = "0.10.2.0"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.0-M4"
libraryDependencies += "com.github.kardapoltsev" %% "json4s-java-time" % "1.0.2"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-RC1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion

trapExit := false
