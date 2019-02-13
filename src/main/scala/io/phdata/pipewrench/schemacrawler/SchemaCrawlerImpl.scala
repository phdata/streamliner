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

  def getCatalog(jdbc: Jdbc): Catalog = {
    val options = getOptions(jdbc)
    SchemaCrawlerUtility.getCatalog(getConnection(jdbc), options)
  }

  def getHtmlOutput(jdbc: Jdbc, outputPath: String): Unit = {
    val path = s"$outputPath/docs"
    val fileName = "schema.html"
    execute(jdbc, TextOutputFormat.html, path, fileName)
  }

  def getErdOutput(jdbc: Jdbc, outputPath: String): Unit = {
    val path = s"$outputPath/docs"
    val fileName = "schema.png"
    execute(jdbc, GraphOutputFormat.png, path, fileName)
  }

  def execute(jdbc: Jdbc, outputFormat: OutputFormat, path: String, fileName: String): Unit = {
    createDir(path)
    val outputOptions =
      OutputOptionsBuilder.newOutputOptions(outputFormat, Paths.get(s"$path/$fileName"))
    val executable = new SchemaCrawlerExecutable("schema")
    executable.setSchemaCrawlerOptions(getOptions(jdbc))
    executable.setOutputOptions(outputOptions)
    executable.setConnection(getConnection(jdbc))
    executable.execute()
  }

  private def getOptions(jdbc: Jdbc): SchemaCrawlerOptions =
    SchemaCrawlerOptionsBuilder
      .builder()
      .withSchemaInfoLevel(SchemaInfoLevelBuilder.standard())
      .includeSchemas(new RegularExpressionInclusionRule(jdbc.schema))
      .tableTypes(jdbc.tableTypes.asJava)
      .toOptions

  private def getConnection(jdbc: Jdbc): Connection = {
    val con = new DatabaseConnectionOptions(jdbc.url)
    con.getConnection(jdbc.username, jdbc.password)
  }

}
