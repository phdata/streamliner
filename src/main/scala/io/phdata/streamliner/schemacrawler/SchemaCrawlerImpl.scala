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

import java.nio.file.Paths
import java.sql.Connection
import java.util.logging.Level
import io.phdata.streamliner.configuration.Jdbc
import io.phdata.streamliner.util.FileUtil
import org.apache.log4j.LogManager
import schemacrawler.inclusionrule.RegularExpressionInclusionRule
import schemacrawler.schema.Catalog
import schemacrawler.schemacrawler.LimitOptionsBuilder
import schemacrawler.schemacrawler.SchemaCrawlerOptions
import schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder
import schemacrawler.tools.command.text.diagram.options.DiagramOutputFormat
import schemacrawler.tools.command.text.schema.options.TextOutputFormat
import schemacrawler.tools.databaseconnector.DatabaseConnectionSource
import schemacrawler.tools.databaseconnector.SingleUseUserCredentials
import schemacrawler.tools.executable.SchemaCrawlerExecutable
import schemacrawler.tools.utility.SchemaCrawlerUtility
import us.fatehi.utility.LoggingConfig
import schemacrawler.tools.options.OutputFormat
import schemacrawler.tools.options.OutputOptionsBuilder

import scala.collection.JavaConverters._

object SchemaCrawlerImpl extends FileUtil {

  def getCatalog(jdbc: Jdbc, password: String): Catalog =
    SchemaCrawlerUtility.getCatalog(getConnection(jdbc, password), getOptions(jdbc))

  def getHtmlOutput(jdbc: Jdbc, password: String, path: String): Unit =
    execute(jdbc, password, TextOutputFormat.html, path, "schema.html")

  def getErdOutput(jdbc: Jdbc, password: String, path: String): Unit =
    execute(jdbc, password, DiagramOutputFormat.png, path, "schema.png")

  def execute(
      jdbc: Jdbc,
      password: String,
      outputFormat: OutputFormat,
      path: String,
      fileName: String): Unit = {
    createDir(path)
    val outputOptions =
      OutputOptionsBuilder.newOutputOptions(outputFormat, Paths.get(s"$path/$fileName"))
    val executable = new SchemaCrawlerExecutable("schema")
    executable.setSchemaCrawlerOptions(getOptions(jdbc))
    executable.setOutputOptions(outputOptions)
    executable.setConnection(getConnection(jdbc, password))
    executable.execute()
  }

  private def setLogLevel(): Unit = {
    if (sys.env.contains("SCHEMA_CRAWLER_DEBUG")) {
      new LoggingConfig(Level.ALL)
    } else {
      val logManager = LogManager.getRootLogger()
      val level = logManager.getLevel.toString match {
        case "OFF" => Level.OFF
        case "ERROR" | "FATAL" | "SEVERE" => Level.SEVERE
        case "WARN" | "WARNING" => Level.WARNING
        case "CONFIG" | "DEBUG" => Level.CONFIG
        case "INFO" => Level.INFO
        case "TRACE" => Level.FINER
        case _ => Level.ALL
      }
      new LoggingConfig(level)
    }
  }

  private def getOptions(jdbc: Jdbc): SchemaCrawlerOptions = {
    setLogLevel()

    val options = SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions()
    val limitOptionsBuilder = LimitOptionsBuilder.builder()
    // latest version of schema crawler requires singular names
    val tableTypes = jdbc.tableTypes
      .map(i =>
        i match {
          case "tables" => "table"
          case "views" => "view"
          case s => s
      })
      .asJava
    // schema crawler now names the objects differently, eg Hive."default"
    limitOptionsBuilder
      .includeSchemas(new RegularExpressionInclusionRule(s".*${jdbc.schema}"))
      .tableTypes(tableTypes)
    jdbc.userDefinedTable match {
      case Some(tables) =>
        val tableList = tables.map(t => s".*${jdbc.schema}\\.${t.name}").mkString("|")
        limitOptionsBuilder.includeTables(new RegularExpressionInclusionRule(s"($tableList)"))
      case None => // no-op
    }
    options.withLimitOptions(limitOptionsBuilder.toOptions)
  }

  private def getConnection(jdbc: Jdbc, password: String): Connection = {
    val dataSource = new DatabaseConnectionSource(jdbc.url)
    dataSource.setUserCredentials(new SingleUseUserCredentials(jdbc.username, password))
    dataSource.get()
  }
}
