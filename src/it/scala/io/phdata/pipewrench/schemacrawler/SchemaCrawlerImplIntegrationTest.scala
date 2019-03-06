/* Copyright 2018 phData Inc. */

package io.phdata.pipewrench.schemacrawler

import java.io.File

import io.phdata.pipewrench.configuration.Jdbc
import org.scalatest.FunSuite

/**
 * This test requires credentials therefore they are ignored.
 * TODO move to integration-test 'it' section.
 * TODO put Oracle into a docker container
 */
class SchemaCrawlerImplIntegrationTest extends FunSuite {

  val jdbc = Jdbc(
    driverClass = None,
    url = "jdbc:oracle:thin:@oraclerds.caewceohkuoi.us-east-1.rds.amazonaws.com:1521:ORCL",
    username = "HR",
    passwordFile = "hdfs:///user/tfoerster/oracle_password",
    schema = "HR",
    tableTypes = Seq("table"),
    None,
    None
  )

  test("Get ERD output") {
    val targetFile = "target/erd"
    SchemaCrawlerImpl.getErdOutput(jdbc, "HR_USER", targetFile)
    assert(new File(targetFile).exists())
  }

  test("Get catalogue") {
    val catalog = SchemaCrawlerImpl.getCatalog(jdbc, "HR_USER")
    assert(catalog.getCrawlInfo != null)
  }

  test("Get HTML output") {
    val targetFile = "target/html"
    SchemaCrawlerImpl.getHtmlOutput(jdbc, "HR_USER", targetFile)
    assert(new File(targetFile).exists())
  }
}
