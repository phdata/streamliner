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

package io.phdata.pipewrench

import java.io.FileNotFoundException

import io.phdata.pipewrench.configuration.ConfigurationBuilder
import io.phdata.pipewrench.configuration.Default
import io.phdata.pipewrench.configuration.YamlSupport
import io.phdata.pipewrench.pipeline.PipelineBuilder
import io.phdata.pipewrench.schemacrawler.SchemaCrawlerImpl
import io.phdata.pipewrench.util.FileUtil
import sf.util.Utility.applyApplicationLogLevel
import java.util.logging.Level

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import scala.io.StdIn

object App extends YamlSupport with Default with FileUtil {

  private val logger = LoggerFactory.getLogger(App.getClass)

  def main(args: Array[String]): Unit = {
    val cli = new Cli(args)
    cli.subcommand match {
      case Some(cli.schema) =>
        val databasePassword = cli.schema.jceksPath.toOption match {
          case Some(path) =>
            val alias = cli.schema.keystoreAlias()
            val conf = new Configuration(true)
            conf.set("hadoop.security.credential.provider.path", path)
            conf.getPassword(alias).mkString("")
          case None =>
            cli.schema.databasePassword.toOption match {
              case Some(password) => password
              case None =>
                print("Enter database password: ")
                StdIn.readLine()
            }
        }

        val schemaLogLevel = cli.schema.schemaLogLevel.toOption match {
          case Some("OFF") => Level.OFF
          case Some("SEVERE" | "FATAL" | "ERROR" | "ERR") => Level.SEVERE
          case Some("WARNING" | "WARN") => Level.WARNING
          case Some("CONFIG" | "DEBUG") => Level.CONFIG
          case Some("INFO") => Level.INFO
          case Some("TRACE") => Level.FINER
          case Some(default) => Level.ALL
        }
        applyApplicationLogLevel(schemaLogLevel)

        val configuration = readConfigurationFile(cli.schema.filePath())
        val outputDirectory =
          configurationOutputDirectory(configuration, cli.schema.outputPath.toOption)
        createDir(outputDirectory)
        val enhancedConfiguration = ConfigurationBuilder.build(configuration, databasePassword)
        writeYamlFile(enhancedConfiguration, s"$outputDirectory/pipewrench-configuration.yml")
        if (cli.schema.createDocs()) {
          SchemaCrawlerImpl
            .getErdOutput(enhancedConfiguration.jdbc, databasePassword, outputDirectory)
          SchemaCrawlerImpl
            .getHtmlOutput(enhancedConfiguration.jdbc, databasePassword, outputDirectory)
        }

      case Some(cli.produceScripts) =>
        val configuration = readConfigurationFile(cli.produceScripts.filePath())
        val typeMappingFile = cli.produceScripts.typeMappingFile()
        val templateDir = cli.produceScripts.templateDirectory()

        if (!fileExists(typeMappingFile)) {
          throw new FileNotFoundException(s"Type mapping file not found: '$typeMappingFile'.")
        }

        if (!directoryExists(templateDir)) {
          throw new FileNotFoundException(s"Template directory not found '$templateDir'")
        }

        val typeMapping = readTypeMappingFile(typeMappingFile)
        createDir(cli.produceScripts.outputPath.getOrElse(OUTPUT_DIRECTORY))
        PipelineBuilder.build(
          configuration,
          typeMapping,
          templateDir,
          cli.produceScripts.outputPath.getOrElse(OUTPUT_DIRECTORY)
        )

      case None =>
        logger.error("Please provide a valid sub command options are `schema` and `scripts`")

    }
  }
}
