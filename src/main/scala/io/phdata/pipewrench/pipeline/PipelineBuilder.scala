package io.phdata.pipewrench.pipeline

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipewrench.configuration._
import io.phdata.pipewrench.util.FileUtil
import org.fusesource.scalate.TemplateEngine

object PipelineBuilder extends FileUtil with Default with LazyLogging {

  private lazy val engine: TemplateEngine = new TemplateEngine

  def build(
      configuration: PipewrenchConfiguration,
      typeMapping: Map[String, Map[String, String]],
      templateDirectory: String,
      outputDirectory: String): Unit = {
    val files = listFilesInDir(s"$templateDirectory/${configuration.configuration.pipeline}")
    val templateFiles = files.filter(f => f.getName.endsWith(".ssp"))
    val nonTemplateFiles = files.filterNot(f => f.getName.endsWith(".ssp"))

    engine.escapeMarkup = false

    configuration.tables.foreach { table =>
      checkConfiguration(configuration.configuration, table)
      val tableDir =
        s"${scriptsDirectory(configuration.configuration, outputDirectory)}/${table.destinationName}"
      createDir(tableDir)

      templateFiles.foreach { templateFile =>
        val rendered = engine.layout(
          templateFile.getPath,
          Map(
            "configuration" -> configuration.configuration,
            "table" -> table,
            "typeMapping" -> typeMapping))
        val replaced = rendered.replace("    ", "\t")
        logger.debug(replaced)
        val fileName = s"$tableDir/${templateFile.getName.replace(".ssp", "")}"
        writeFile(replaced, fileName)
        isExecutable(fileName)
      }
      nonTemplateFiles.foreach { nonTemplateFile =>
        val content = readFile(nonTemplateFile.getPath)
        val fileName = s"$tableDir/${nonTemplateFile.getName}"
        writeFile(content, fileName)
        isExecutable(fileName)
      }
    }
    writeSchemaMakeFile(configuration, templateDirectory, outputDirectory)
  }

  private def writeSchemaMakeFile(
      configuration: PipewrenchConfiguration,
      templateDirectory: String,
      outputDirectory: String): Unit = {
    val templateFile = s"$templateDirectory/Makefile.ssp"
    val rendered = engine.layout(templateFile, Map("tables" -> configuration.tables))
    val replaced = rendered.replace("    ", "\t")
    logger.debug(replaced)
    writeFile(
      replaced,
      s"${scriptsDirectory(configuration.configuration, outputDirectory)}/Makefile")
  }

  private def isExecutable(path: String): Unit = {
    if (path.contains(".sh")) {
      setExecutable(path)
    }
  }

  private def checkConfiguration(configuration: Configuration, table: TableDefinition): Unit = {
    if (configuration.pipeline.equalsIgnoreCase("INCREMENTAL-WITH-KUDU")) {
      checkPrimaryKeys(table)
      checkCheckColumn(table)
    }
    if (configuration.pipeline.equalsIgnoreCase("KUDU-TABLE-DDL")) {
      checkPrimaryKeys(table)
    }
  }

  private def checkCheckColumn(table: TableDefinition): Unit = {
    if (table.checkColumn.isEmpty) {
      throw new RuntimeException(
        s"Check column for table: ${table.sourceName} is empty, cannot execute incremental sqoop job")
    }
  }

  private def checkPrimaryKeys(table: TableDefinition): Unit = {
    if (table.primaryKeys.isEmpty) {
      throw new RuntimeException(
        s"Primary keys for table: ${table.sourceName} are empty, cannot create Kudu table.")
    }
  }
}
