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

package io.phdata.pipewrench.pipeline

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipewrench.configuration._
import io.phdata.pipewrench.util.FileUtil
import org.fusesource.scalate.TemplateEngine

object PipelineBuilder extends FileUtil with Default with LazyLogging {

  private lazy val engine: TemplateEngine = new TemplateEngine

  def build(
      configuration: Configuration,
      typeMapping: Map[String, Map[String, String]],
      templateDirectory: String,
      outputDirectory: String): Unit = {
    val files = listFilesInDir(s"$templateDirectory/${configuration.pipeline}")
    val templateFiles = files.filter(f => f.getName.endsWith(".ssp"))
    val nonTemplateFiles = files.filterNot(f => f.getName.endsWith(".ssp"))

    engine.escapeMarkup = false

    configuration.tables match {
      case Some(tables) =>
        tables.foreach { table =>
          checkConfiguration(configuration, table)
          val tableDir =
            s"${scriptsDirectory(configuration, outputDirectory)}/${table.destinationName}"
          createDir(tableDir)

          templateFiles.foreach { templateFile =>
            logger.info(s"Rendering file $templateFile")
            val rendered = engine.layout(
              templateFile.getPath,
              Map("configuration" -> configuration, "table" -> table, "typeMapping" -> typeMapping))
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
      case None =>
        throw new RuntimeException(
          "Tables section is not found. Check the configuration (example: pipewrench-configuration.yml) file")
    }
    writeSchemaMakeFile(configuration, typeMapping, templateDirectory, outputDirectory)
  }

  private def writeSchemaMakeFile(
      configuration: Configuration,
      typeMapping: Map[String, Map[String, String]],
      templateDirectory: String,
      outputDirectory: String): Unit = {

    //Makefile.ssp support left in for backwards compatibility
    val makefile = s"$templateDirectory/Makefile.ssp"
    if (fileExists(makefile)) {
      val rendered = engine.layout(makefile, Map("tables" -> configuration.tables.get))
      val replaced = rendered.replace("    ", "\t")
      logger.debug(replaced)
      writeFile(replaced, s"${scriptsDirectory(configuration, outputDirectory)}/Makefile")
    }

    val schemaFiles =
      listFilesInDir(s"$templateDirectory").filter(file => file.getName.contains(".schema"))
    val schemaTemplateFiles = schemaFiles.filter(f => f.getName.endsWith(".ssp"))
    schemaTemplateFiles.foreach { templateFile =>
      logger.info(s"Rendering file $templateFile")
      val rendered = engine.layout(
        templateFile.getPath,
        Map(
          "configuration" -> configuration,
          "tables" -> configuration.tables.get,
          "typeMapping" -> typeMapping))
      logger.debug(s"Rendered file $rendered")
      val fileName =
        s"${scriptsDirectory(configuration, outputDirectory)}/${templateFile.getName.replace(".ssp", "").replace(".schema", "")}"
      writeFile(rendered, fileName)
      isExecutable(fileName)
    }
  }

  private def isExecutable(path: String): Unit = {
    val file = new File(path)
    logger.debug(s"File executable flag ${file.canExecute()}: '$path'")
    if (file.canExecute) {
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
        s"'checkColumn' for table: ${table.sourceName} is empty, cannot execute incremental sqoop job")
    }
  }

  private def checkPrimaryKeys(table: TableDefinition): Unit = {
    if (table.primaryKeys.isEmpty) {
      throw new RuntimeException(
        s"Primary keys for table: ${table.sourceName} are empty, cannot create Kudu table.")
    }
  }
}
