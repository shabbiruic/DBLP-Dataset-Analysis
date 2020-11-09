import sbt.util

name := "DBLPMapReduce"

version := "0.1"

scalaVersion := "2.13.3"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "DBLPMapReduce" + "." + artifact.extension
}
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.0.3"

libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.5"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.0" % "test"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.3.0"
logLevel := util.Level.Info