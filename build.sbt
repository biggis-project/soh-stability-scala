// Rename this as you see fit
name := "transformation"

version := "0.1.0"

scalaVersion := "2.11.8"

organization := "com.non"

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-Yinline-warnings",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials",
  "-feature")

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

resolvers += Resolver.bintrayRepo("azavea", "geotrellis")

libraryDependencies ++= Seq(
  "com.azavea.geotrellis" %% "geotrellis-spark" % "1.0.0-d9f051d",
  "org.apache.spark"      %% "spark-core"       % "2.0.1" % "provided",
  "org.scalatest"         %%  "scalatest"       % "2.2.0" % "test",
  "net.postgis" % "postgis-jdbc" % "2.1.7.2",
  "com.typesafe.slick" % "slick_2.10.1" % "2.0.0-M1",
  "org.mockito" % "mockito-all" % "1.8.4",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "net.sf.opencsv" % "opencsv" % "2.3",
  "de.lmu.ifi.dbs.elki" % "elki" % "0.7.1",
  "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.5.0"

)

//mainClass in (Compile, run) := Some("demo.Main")
mainClass in (Compile, run) := Some("scripts.MetrikValidation")


