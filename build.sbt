name := "pipewrench"
version := "0.1"
organization := "io.phdata.pipewrench"
scalaVersion := "2.11.12"

val schemaCrawlerVersion = "15.03.04"

scalafmtOnCompile := true

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.rogach" %% "scallop" % "3.1.1",
  "net.jcazevedo" %% "moultingyaml" % "0.4.0",
  "us.fatehi" % "schemacrawler-mysql" % schemaCrawlerVersion,
  "us.fatehi" % "schemacrawler-postgresql" % schemaCrawlerVersion,
  "us.fatehi" % "schemacrawler-oracle" % schemaCrawlerVersion,
  "us.fatehi" % "schemacrawler-db2" % schemaCrawlerVersion,
  "us.fatehi" % "schemacrawler-sqlserver" % schemaCrawlerVersion,
  "org.scalatra.scalate" %% "scalate-core" % "1.9.0"
)

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" * "*").get.map { f =>
    f -> s"conf/${f.name}"
  }
}
mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" / "templates" / "truncate-reload" * "*").get.map { f =>
    f -> s"templates/truncate-reload/${f.name}"
  }
}

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" / "templates" / "kudu-table-ddl" * "*").get.map { f =>
    f -> s"templates/kudu-table-ddl/${f.name}"
  }
}

mappings in Universal ++= {
  ((sourceDirectory in Compile).value / "resources" / "templates" / "incremental-with-kudu" * "*").get.map { f =>
    f -> s"templates/incremental-with-kudu/${f.name}"
  }
}

enablePlugins(JavaServerAppPackaging, UniversalDeployPlugin)