name := "graphframes"

version := "0.1-SNAPSHOT"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0-SNAPSHOT"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4"

// Run tests with more memory
javaOptions in test += "-Xmx2G"

fork := true
