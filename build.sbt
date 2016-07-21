name := "spark_assignment_1"

version := "1.0"


val spark = "org.apache.spark" %% "spark-core" % "2.0.0-preview"

lazy val commonSettings = Seq(
  organization := "com.knoldus.spark",
  version := "0.1.0",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "spark_assignment_1",
    libraryDependencies ++= Seq(spark)
  )

