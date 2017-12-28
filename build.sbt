//
// http://spark.apache.org/docs/latest/quick-start.html#a-standalone-app-in-scala
//
name := "Movie Rater"

version := "1.0"

// 2.11 doesn't seem to work
scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % sparkVersion

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
