name := "Spark Project"

version := "1.0"

scalaVersion := "2.11.8"
val kafkaVersion = "0.10.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.0-M4"
libraryDependencies += "com.github.kardapoltsev" %% "json4s-java-time" % "1.0.2"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion

// If you want to activate the log4j.properties file:
// fork in run := true
// javaOptions in run ++= Seq(
//   "-Dlog4j.debug=true",
//   "-Dlog4j.configuration=log4j.properties")
// outputStrategy := Some(StdoutOutput)
