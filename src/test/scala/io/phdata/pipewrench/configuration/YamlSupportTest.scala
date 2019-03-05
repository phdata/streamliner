/* Copyright 2018 phData Inc. */

package io.phdata.pipewrench.configuration

import org.scalatest.FunSuite

class YamlSupportTest extends FunSuite with YamlSupport {
  val jdbc = Jdbc(Some("driver"), "foo", "user", "pass", "schema", Seq("views"), Some(Seq()), None)

  val hadoop = Hadoop("", Database("name", "path"), Database("name", "path"))

  val configuration =
    Configuration(name = "foo", environment = "dev", pipeline = "p1", jdbc, hadoop)

  val typeMapping = Map(
    "bigint" -> Map(
      "kudu" -> "bigint",
      "impala" -> "bigint",
      "parquet" -> "bigint",
      "avro" -> "bigint"),
    "tinyint" -> Map("kudu" -> "int", "impala" -> "int", "parquet" -> "int", "avro" -> "int"),
    "decimal" -> Map(
      "kudu" -> "string",
      "impala" -> "decimal",
      "parquet" -> "decimal",
      "avro" -> "string")
  )

  val configurationString =
    """hadoop:
      |  impalaShellCommand: ''
      |  stagingDatabase:
      |    name: name
      |    path: path
      |  reportingDatabase:
      |    name: name
      |    path: path
      |jdbc:
      |  schema: schema
      |  username: user
      |  tableTypes:
      |  - views
      |  tables: []
      |  passwordFile: pass
      |  url: foo
      |  driverClass: driver
      |name: foo
      |pipeline: p1
      |environment: dev
    """.stripMargin

  test("Parse configuration") {
    assertResult("dev")(parseConfiguration(configurationString).environment)
  }

  test("Parse type mapping") {
    val mapping = """bigint:
                    |  kudu: bigint
                    |  impala: bigint
                    |  parquet: bigint
                    |  avro: bigint
                    |tinyint:
                    |  kudu: int
                    |  impala: int
                    |  parquet: int
                    |  avro: int
                    |decimal:
                    |  kudu: string
                    |  impala: decimal
                    |  parquet: decimal
                    |  avro: string
                    """.stripMargin

    // if it doesn't blow up we've succeeded
    assert(typeMapping.isInstanceOf[TypeMapping])
  }

  test("Print yaml") {
    val expected =
      """hadoop:
        |  impalaShellCommand: ''
        |  stagingDatabase:
        |    name: name
        |    path: path
        |  reportingDatabase:
        |    name: name
        |    path: path
        |jdbc:
        |  schema: schema
        |  username: user
        |  tableTypes:
        |  - views
        |  tables: []
        |  passwordFile: pass
        |  url: foo
        |  driverClass: driver
        |name: foo
        |pipeline: p1
        |environment: dev
      """.stripMargin

    // if it doesn't blow up we've succeeded
    assert(prettyPrintConfiguration(configuration).isInstanceOf[String])
  }

}
