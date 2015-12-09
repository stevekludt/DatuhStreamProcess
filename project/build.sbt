name := "DatuhStreamProcess"

version := "1.0"

scalaVersion := "2.11.7"

val json4sNative = "org.json4s" %% "json4s-native" % "3.2.10"
val json4sJackson = "org.json4s" %% "json4s-jackson" % "3.2.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1",
  "org.apache.spark" %% "spark-sql" % "1.3.1",
  "org.apache.spark" %% "spark-hive" % "1.3.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.1",
  "org.scala-lang" % "scala-reflect" % "2.11.7")