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
ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation")

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

lazy val controller = (project in file("controller"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(`bytegraph`)
  .settings(
    name := "controller",
    libraryDependencies ++= Seq(
      "com.twitter" %% "finagle-http" % "24.2.0",
      "com.twitter" %% "finagle-http2" % "24.2.0",
      "com.twitter" %% "finagle-stats" % "24.2.0",
      "com.typesafe" % "config" % "1.4.3",
      "org.apache.kafka" % "kafka-clients" % "3.6.2",
      "org.apache.helix" % "helix-core" % "1.4.0" excludeAll ExclusionRule("org.slf4j", "slf4j-log4j12"),
      "org.apache.helix" % "helix-rest" % "1.4.0" excludeAll ExclusionRule("org.slf4j", "slf4j-log4j12"),
      "com.typesafe.akka" %% "akka-actor" % "2.8.6",
      "ch.qos.logback" % "logback-classic" % "1.5.7",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )
  )
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "io.exsql.datastore.controller"
  )

lazy val replica = (project in file("replica"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(`bytegraph`)
  .settings(
    name := "controller",
    libraryDependencies ++= Seq(
      "com.twitter" %% "finagle-http" % "24.2.0",
      "com.twitter" %% "finagle-http2" % "24.2.0",
      "com.twitter" %% "finagle-stats" % "24.2.0",
      "com.typesafe" % "config" % "1.4.3",
      "org.apache.kafka" %% "kafka-streams-scala" % "3.6.2",
      "org.apache.kafka" % "kafka-clients" % "3.6.2",
      "org.apache.helix" % "helix-core" % "1.4.0" excludeAll ExclusionRule("org.slf4j", "slf4j-log4j12"),
      "com.squareup.okhttp3" % "okhttp" % "4.12.0",
      "com.typesafe.akka" %% "akka-actor" % "2.8.6",
      "org.rocksdb" % "rocksdbjni" % "9.5.2",
      "com.github.pathikrit" %% "better-files" % "3.9.2",
      "org.apache.spark" %% "spark-sql" % "3.3.4" excludeAll ExclusionRule("org.slf4j", "slf4j-log4j12"),
      "ch.qos.logback" % "logback-classic" % "1.5.7",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )
  )
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "io.exsql.datastore.replica"
  )

lazy val datastore = (project in file("."))
  .dependsOn(`bytegraph`)
  .aggregate(`bytegraph`, `controller`, `replica`)
  .settings(
    name := "datastore"
  )
