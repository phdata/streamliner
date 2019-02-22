package io.phdata.pipewrench.schemacrawler

import java.nio.file.Paths
import java.sql.Connection

import io.phdata.pipewrench.configuration.Jdbc
import io.phdata.pipewrench.util.FileUtil
import schemacrawler.schema.Catalog
import schemacrawler.schemacrawler._
import schemacrawler.tools.executable.SchemaCrawlerExecutable
import schemacrawler.tools.integration.graph.GraphOutputFormat
import schemacrawler.tools.options.OutputFormat
import schemacrawler.tools.options.OutputOptionsBuilder
import schemacrawler.tools.options.TextOutputFormat
import schemacrawler.utility.SchemaCrawlerUtility

import collection.JavaConverters._

object SchemaCrawlerImpl extends FileUtil {

  def getCatalog(jdbc: Jdbc, password: String): Catalog = {
    val options = getOptions(jdbc)
    SchemaCrawlerUtility.getCatalog(getConnection(jdbc, password), options)
  }

  def getHtmlOutput(jdbc: Jdbc, password: String, outputPath: String): Unit = {
    val path = s"$outputPath/docs"
    val fileName = "schema.html"
    execute(jdbc, password, TextOutputFormat.html, path, fileName)
  }

  def getErdOutput(jdbc: Jdbc, password: String, outputPath: String): Unit = {
    val path = s"$outputPath/docs"
    val fileName = "schema.png"
    execute(jdbc, password, GraphOutputFormat.png, path, fileName)
  }

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

  private def getOptions(jdbc: Jdbc): SchemaCrawlerOptions = {
    val options = SchemaCrawlerOptionsBuilder
      .builder()
      .withSchemaInfoLevel(SchemaInfoLevelBuilder.standard())
      .includeSchemas(new RegularExpressionInclusionRule(jdbc.schema))
      .tableTypes(jdbc.tableTypes.asJava)
    jdbc.tables match {
      case Some(tables) =>
        val tableList = tables.map(t => s"${jdbc.schema}.${t.name}.*").mkString("|")
        options.includeTables(new RegularExpressionInclusionRule(s"($tableList)"))
      case None => // no-op
    }
    options.toOptions
  }

  private def getConnection(jdbc: Jdbc, password: String): Connection = {
    val con = new DatabaseConnectionOptions(jdbc.url)
    con.getConnection(jdbc.username, password)
  }

}
