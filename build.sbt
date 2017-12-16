crossScalaVersions := Seq("2.12.4", "2.11.12")

scalaVersion in Global := crossScalaVersions.value.head

organization := "com.hypertino"

name := "hyperbus-t-zeromq"

version := "0.6-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.hypertino"   %% "hyperbus" % "0.6-SNAPSHOT",
  "org.zeromq" % "jeromq" % "0.4.3",
  "org.scalamock"   %% "scalamock-scalatest-support" % "3.5.0" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.8" % "test",
  compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

fork in Test := true

parallelExecution in Test := false