import sbt._

import scala.io.Source

def versionFromFile: String = Source.fromFile("version").getLines().mkString("")
def snapshot: Boolean = versionFromFile.endsWith("-SNAPSHOT")

name := "streamliner"
version := versionFromFile
isSnapshot := snapshot
organization := "io.phdata.streamliner"
scalaVersion := "2.12.12"

val schemaCrawlerVersion = "16.2.5"

val excludeSlf4jBinding = ExclusionRule(organization = "org.slf4j")

lazy val root = (project in file("."))
  .settings(scalafmtOnCompile := true)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.30",
      "org.slf4j" % "slf4j-log4j12" % "1.7.30",
      "org.rogach" %% "scallop" % "3.1.1",
      "io.circe" %% "circe-yaml" % "0.13.1",
      "io.circe" %% "circe-generic" % "0.13.0",
      "us.fatehi" % "schemacrawler-mysql" % schemaCrawlerVersion excludeAll(excludeSlf4jBinding),
      "us.fatehi" % "schemacrawler-postgresql" % schemaCrawlerVersion excludeAll(excludeSlf4jBinding),
      "us.fatehi" % "schemacrawler-oracle" % schemaCrawlerVersion excludeAll(excludeSlf4jBinding),
      "us.fatehi" % "schemacrawler-db2" % schemaCrawlerVersion excludeAll(excludeSlf4jBinding),
      "us.fatehi" % "schemacrawler-sqlserver" % schemaCrawlerVersion excludeAll(excludeSlf4jBinding),
      "guru.nidi" % "graphviz-java" % "0.8.1",
      "org.scalatra.scalate" %% "scalate-core" % "1.9.0",
      "com.amazonaws" % "aws-java-sdk-glue" % "1.11.774",
      "org.scalatest" %% "scalatest" % "3.0.5" % "it,test"
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

mainClass in Compile := Some("io.phdata.streamliner.App")

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
