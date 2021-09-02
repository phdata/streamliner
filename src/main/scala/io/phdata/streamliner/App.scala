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

package io.phdata.streamliner

import io.phdata.streamliner.schemadefiner.configbuilder.GenerateStateCommand
import io.phdata.streamliner.schemadefiner.configbuilder.SchemaCommand
import io.phdata.streamliner.schemadefiner.configbuilder.ScriptCommand
import org.apache.log4j.LogManager

object App {

  private val logger = LogManager.getLogger(App.getClass)

  def main(args: Array[String]): Unit = {
    val cli = new Cli(args)
    cli.subcommand match {
      case Some(cli.schema) =>
        SchemaCommand.build(
          cli.schema.config(),
          cli.schema.stateDirectory(),
          cli.schema.databasePassword.getOrElse(""),
          cli.schema.createDocs.getOrElse(false),
          cli.schema.previousStateDirectory.getOrElse(null)
        )

      case Some(cli.produceScripts) =>
        ScriptCommand.build(
          cli.produceScripts.config(),
          cli.produceScripts.stateDirectory(),
          cli.produceScripts.previousStateDirectory(),
          cli.produceScripts.typeMappingFile(),
          cli.produceScripts.templateDirectory(),
          cli.produceScripts.outputPath()
        )

      case Some(cli.generateState) =>
        GenerateStateCommand.build(
          cli.generateState.tableExclude.getOrElse(null),
          cli.generateState.tableInclude.getOrElse(null),
          cli.generateState.columnInclude.getOrElse(null),
          cli.generateState.columnExclude.getOrElse(null),
          cli.generateState.tableNameRemove.getOrElse(null),
          cli.generateState.columnNameRemove.getOrElse(null),
          cli.generateState.outputPath(),
          cli.generateState.sourceStateDirectory(),
          cli.generateState.tableCsvPath(),
          cli.generateState.columnCsvPath(),
        )

      case None =>
        logger.error("Please provide a valid sub command options are `schema` and `scripts`")

    }
  }

}
