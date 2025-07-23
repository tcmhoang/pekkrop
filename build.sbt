val PekkoVersion = "1.1.5"


ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "pekkrop",
    run / javaOptions ++= Seq("-Xms1024m", "-Xmx1024m", "-XX:ReservedCodeCacheSize=128m", "-XX:MaxMetaspaceSize=256m", "-Xss2m", "-Dfile.encoding=UTF-8"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-typed" % PekkoVersion,
      "ch.qos.logback" % "logback-classic" % "1.5.18"
    ),
    run / fork := false,
    Global / cancelable := false,

  )

