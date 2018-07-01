name := "Spark Project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.0-M4"
libraryDependencies += "com.github.kardapoltsev" %% "json4s-java-time" % "1.0.2"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"
