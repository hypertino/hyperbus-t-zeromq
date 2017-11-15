crossScalaVersions := Seq("2.12.3", "2.11.11")

scalaVersion in Global := crossScalaVersions.value.head

organization := "com.hypertino"

name := "hyperbus-t-zeromq"

version := "0.4-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.hypertino"   %% "hyperbus" % "0.4-SNAPSHOT",
  "org.zeromq" % "jeromq" % "0.4.0",
  "org.scalamock"   %% "scalamock-scalatest-support" % "3.5.0" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.8" % "test",
  compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

fork in Test := true

parallelExecution in Test := false