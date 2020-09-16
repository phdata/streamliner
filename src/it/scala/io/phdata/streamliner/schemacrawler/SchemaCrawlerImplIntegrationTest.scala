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

package io.phdata.streamliner.schemacrawler

import java.io.File

import io.phdata.streamliner.configuration.{Jdbc, SnowflakeUserDefinedTable, UserDefinedTable}
import org.scalatest.FunSuite

/**
 * This test requires credentials therefore they are ignored.
 */
class SchemaCrawlerImplIntegrationTest extends FunSuite {

  val jdbc = Jdbc(
    `type` = "Jdbc",
    driverClass = None,
    url = "jdbc:mysql://localhost:3306/employees",
    username = "root",
    passwordFile = "",
    jceKeyStorePath=None,
    keystoreAlias = None,
    schema = "employees",
    tableTypes = Seq("table"),
    None,
    None
  )

  test("Get ERD output") {
    val targetFile = "target/erd"
    SchemaCrawlerImpl.getErdOutput(jdbc, "streamliner", targetFile)
    assert(new File(targetFile).exists())
  }

  test("Get catalogue") {
    val catalog = SchemaCrawlerImpl.getCatalog(jdbc, "streamliner")
    assert(catalog.getCrawlInfo != null)
  }

  test("Get HTML output") {
    val targetFile = "target/html"
    SchemaCrawlerImpl.getHtmlOutput(jdbc, "streamliner", targetFile)
    assert(new File(targetFile).exists())
  }
}
