name := "cloud-storage-gatling"
version := "0.1.0"
enablePlugins(GatlingPlugin)
scalaVersion := "2.12.10"
scalacOptions := Seq(
  "-encoding",
  "UTF-8",
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:implicitConversions",
  "-language:postfixOps"
)
libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.106.0",
  "com.google.cloud" % "google-cloud-datastore" % "1.102.4",
  "io.gatling.highcharts" % "gatling-charts-highcharts" % "3.3.1" % "test,it",
  "io.gatling" % "gatling-test-framework" % "3.3.1" % "test,it"
)

logLevel := Level.Warn
