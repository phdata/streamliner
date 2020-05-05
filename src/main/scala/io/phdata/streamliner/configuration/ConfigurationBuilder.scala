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

package io.phdata.streamliner.configuration

import io.phdata.streamliner.schemacrawler.SchemaCrawlerImpl
import org.apache.log4j.Logger

object ConfigurationBuilder extends YamlSupport {

  private lazy val logger = Logger.getLogger(ConfigurationBuilder.getClass)

  def build(configurationFile: String, outputDirectory: Option[String] = None, passwordOpt: Option[String] = None, createDocs: Boolean = false): Unit =
    build(readConfigurationFile(configurationFile), outputDirectory, passwordOpt, createDocs)

  def build(configuration: Configuration, outputDirectory: Option[String], passwordOpt: Option[String], createDocs: Boolean): Unit = {
    val enhancedConfiguration = configuration.source match {
      case jdbc: Jdbc =>
        passwordOpt match {
          case Some(password) =>
            if (createDocs) {
              writeDocs(configuration, password, outputDirectory)
            }
            JDBCParser.parse(configuration, password)
          case None => throw new RuntimeException("A databasePassword is required when crawling JDBC sources")
        }
      case glue: GlueCatalog => GlueCatalogParser.parse(configuration)
    }
    write(enhancedConfiguration, outputDirectory)
  }

  private def write(configuration: Configuration, outputDirectory: Option[String] = None): Unit = {
    val dir = outputDirectory.fold(
      s"output/${configuration.name}/${configuration.environment}/conf")(output => s"$output/conf")
    writeConfiguration(configuration, dir)
  }

  private def writeDocs(
      configuration: Configuration,
      databasePassword: String,
      outputDirectory: Option[String] = None): Unit = {
    val path =
      outputDirectory.fold(s"output/${configuration.name}/${configuration.environment}/docs")(p =>
        s"$p/docs")
    val jdbc = configuration.source.asInstanceOf[Jdbc]
    SchemaCrawlerImpl.getErdOutput(jdbc, databasePassword, path)
    SchemaCrawlerImpl.getHtmlOutput(jdbc, databasePassword, path)
  }

}
