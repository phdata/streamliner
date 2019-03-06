/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
