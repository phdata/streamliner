import sbt._

import scala.io.Source

def versionFromFile: String = Source.fromFile("version").getLines().mkString("")
def snapshot: Boolean = versionFromFile.endsWith("-SNAPSHOT")

name := "pipewrench"
version := versionFromFile
isSnapshot := snapshot
organization := "io.phdata.pipewrench"
scalaVersion := "2.11.12"

val schemaCrawlerVersion = "16.2.5"

lazy val root = (project in file("."))
  .settings(scalafmtOnCompile := true)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "org.rogach" %% "scallop" % "3.1.1",
      "io.circe" %% "circe-yaml" % "0.8.0",
      "io.circe" %% "circe-generic" % "0.8.0",
      "us.fatehi" % "schemacrawler-mysql" % schemaCrawlerVersion,
      "us.fatehi" % "schemacrawler-postgresql" % schemaCrawlerVersion,
      "us.fatehi" % "schemacrawler-oracle" % schemaCrawlerVersion,
      "us.fatehi" % "schemacrawler-db2" % schemaCrawlerVersion,
      "us.fatehi" % "schemacrawler-sqlserver" % schemaCrawlerVersion,
      "guru.nidi" % "graphviz-java" % "0.8.1",
      "org.scalatra.scalate" %% "scalate-core" % "1.9.0",
      "org.scalatest" %% "scalatest" % "3.0.5" % "it,test",
      "org.apache.hadoop" % "hadoop-common" % "2.8.3"
    )
  )

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" * "*").get.map { f =>
    f -> s"conf/${f.name}"
  }
}

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" / "templates" * "*").get.map { f =>
    f -> s"templates/${f.name}"
  }
}

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" / "templates" / "truncate-reload" * "*").get
    .map { f =>
      f -> s"templates/truncate-reload/${f.name}"
    }
}

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" / "templates" / "kudu-table-ddl" * "*").get
    .map { f =>
      f -> s"templates/kudu-table-ddl/${f.name}"
    }
}

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" / "templates" / "incremental-with-kudu" * "*").get
    .map { f =>
      f -> s"templates/incremental-with-kudu/${f.name}"
    }
}

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" / "templates" / "snowflake-dms-cdc" * "*").get
    .map { f =>
      f -> s"templates/snowflake-dms-cdc/${f.name}"
    }
}

enablePlugins(JavaServerAppPackaging, UniversalDeployPlugin, RpmArtifactoryDeployPlugin)

mainClass in Compile := Some("io.phdata.pipewrench.App")

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

val artifactoryUrl = "https://repository.phdata.io/artifactory/list"

publishTo := {
  if (isSnapshot.value)
    Some(
      "phData Snapshots".at(
        s"$artifactoryUrl/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime))
  else
    Some("phData Releases".at(s"$artifactoryUrl/libs-release-local"))
}

publish in Rpm := (rpmArtifactoryPublish in Rpm).value

rpmLicense := Some("License: GPLv2")
rpmVendor := "phData"
rpmArtifactoryUrl in Rpm := artifactoryUrl
rpmArtifactoryRepo in Rpm := {
  if (isSnapshot.value) {
    "rpm-unstable"
  } else {
    "rpm-stable"
  }
}
rpmArtifactoryCredentials in Rpm := Some(credentials.value.head)

rpmArtifactoryPath in Rpm := s"${packageName.value}"
