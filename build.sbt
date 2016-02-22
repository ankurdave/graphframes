// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

scalaVersion := "2.11.7"

sparkVersion := "2.0.0-SNAPSHOT"

spName := "graphframes/graphframes"

// Don't forget to set the version
version := "0.1.0-SNAPSHOT"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))


// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("graphx", "sql")

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

parallelExecution := false

unmanagedSourceDirectories in Compile ++=
  Seq(baseDirectory.value / "src" / "main" / (if (sparkVersion.value.substring(0, 3) == "1.4") "spark-1.4" else "spark-x"))

unmanagedSourceDirectories in Test ++=
  Seq(baseDirectory.value / "src" / "test" / (if (sparkVersion.value.substring(0, 3) == "1.4") "spark-1.4" else "spark-x"))

scalacOptions in (Compile, doc) ++= Seq("-groups", "-implicits")

scalacOptions in (Test, doc) ++= Seq("-groups", "-implicits")

autoAPIMappings := true
