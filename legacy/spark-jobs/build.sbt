name := "legacy-spark-jobs"
version := "0.1.0"
scalaVersion := "2.12.18"
val sparkV = "3.5.3"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkV % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV % "provided"
)
