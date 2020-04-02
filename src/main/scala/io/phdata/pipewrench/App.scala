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

import io.phdata.pipewrench.configuration.ConfigurationBuilder
import io.phdata.pipewrench.pipeline.PipelineBuilder

import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

import scala.io.StdIn

object App {

  private val logger = LoggerFactory.getLogger(App.getClass)

  def main(args: Array[String]): Unit = {
    val cli = new Cli(args)
    cli.subcommand match {
      case Some(cli.schema) =>
        val databasePassword = getPassword(
          cli.schema.databasePassword.toOption,
          cli.schema.jceksPath.toOption,
          cli.schema.keystoreAlias.toOption)

        val configuration = ConfigurationBuilder.build(cli.schema.filePath(), databasePassword)
        ConfigurationBuilder.write(configuration, cli.schema.outputPath.toOption)

        if (cli.schema.createDocs()) {
          ConfigurationBuilder.writeDocs(configuration, databasePassword, cli.schema.outputPath.toOption)
        }

      case Some(cli.produceScripts) =>

        PipelineBuilder.build(
          cli.produceScripts.filePath(),
          cli.produceScripts.typeMappingFile(),
          cli.produceScripts.templateDirectory(),
          cli.produceScripts.outputPath.toOption
        )

      case None =>
        logger.error("Please provide a valid sub command options are `schema` and `scripts`")

    }
  }

  private def getPassword(cliPassword: Option[String], jceksPath: Option[String], keyStoreAlias: Option[String]): String = {
    jceksPath match {
      case Some(path) =>
        val alias = keyStoreAlias.get
        val conf = new Configuration(true)
        conf.set("hadoop.security.credential.provider.path", path)
        conf.getPassword(alias).mkString("")
      case None =>
        cliPassword match {
          case Some(password) => password
          case None =>
            print("Enter database password: ")
            StdIn.readLine()
        }
    }
  }

}
