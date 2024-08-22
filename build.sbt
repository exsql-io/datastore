ThisBuild / organization := "io.exsql"
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / version      := "0.0.1"
ThisBuild / Test / fork := true
ThisBuild / Test / parallelExecution := false
ThisBuild / compileOrder := CompileOrder.Mixed
ThisBuild / coverageExcludedPackages := ""
ThisBuild / coverageFailOnMinimum := true
ThisBuild / coverageMinimumStmtTotal := 0
ThisBuild / coverageMinimumBranchTotal := 0

lazy val bytegraph = (project in file("bytegraph"))
  .settings(
    name := "bytegraph",
    libraryDependencies ++= Seq(
      "com.jsoniter" % "jsoniter" % "0.9.23",
      "org.slf4j" % "slf4j-api" % "2.0.16",
      "org.codehaus.janino" % "janino" % "3.1.12",
      "net.openhft" % "zero-allocation-hashing" % "0.16",
      "com.google.googlejavaformat" % "google-java-format" % "1.6",
      "it.unimi.dsi" % "fastutil" % "8.5.14",
      "commons-codec" % "commons-codec" % "1.17.1",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "ch.qos.logback" % "logback-classic" % "1.5.7" % Test
    )
  )

lazy val `bytegraph-bench` = (project in file("bytegraph/bench"))
  .dependsOn(`bytegraph`)
  .enablePlugins(JmhPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.jsoniter" % "jsoniter" % "0.9.23",
      "org.javassist" % "javassist" % "3.30.2-GA"
    ),
    javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
  )

lazy val datastore = (project in file("."))
  .dependsOn(`bytegraph`)
  .aggregate(`bytegraph`)
  .settings(
    name := "datastore"
  )
