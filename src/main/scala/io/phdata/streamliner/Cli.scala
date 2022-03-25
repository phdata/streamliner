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

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.Subcommand

class Cli(args: Seq[String]) extends ScallopConf(args) {

  val schema = new Subcommand("schema") {

    val config: ScallopOption[String] =
      opt[String]("config", descr = "Path to ingest configuration", required = true)

    val stateDirectory: ScallopOption[String] = opt[String](
      "state-directory",
      descr =
        "Current run table config directory where Streamliner configuration per table will be written.",
      required = true
    )

    val previousStateDirectory: ScallopOption[String] = opt[String](
      "previous-state-directory",
      descr =
        "Previous run table config directory where Streamliner configuration per table is written to",
      required = false
    )

    val databasePassword: ScallopOption[String] = opt[String](
      "database-password",
      descr = "Database password",
      required = false
    )

    val createDocs: ScallopOption[Boolean] = opt[Boolean](
      "create-docs",
      descr = "Flag to indicate whether the HTML and ERD documentation should be produced",
      required = false,
      default = Some(false)
    )

    val logLevel: ScallopOption[String] = opt[String](
      "log-level",
      descr = "Parameter to change the application log level",
      required = false
    )
  }

  val produceScripts = new Subcommand("scripts") {

    val config: ScallopOption[String] =
      opt[String]("config", descr = "Path to ingest configuration", required = true)

    val stateDirectory: ScallopOption[String] = opt[String](
      "state-directory",
      descr =
        "Current run table config directory where Streamliner configuration per table will be written.",
      required = true
    )

    val previousStateDirectory: ScallopOption[String] = opt[String](
      "previous-state-directory",
      descr =
        "Previous run table config directory where Streamliner configuration per table is written to",
      required = true
    )

    val outputPath: ScallopOption[String] = opt[String](
      "output-path",
      descr = "Directory path where scripts should be written to",
      required = true
    )

    val typeMappingFile: ScallopOption[String] =
      opt[String]("type-mapping", descr = "Path to data type mapping file", required = true)

    val templateDirectory: ScallopOption[String] = opt[String](
      "template-directory",
      descr = "Template directory path",
      required = true
    )

    val logLevel: ScallopOption[String] = opt[String](
      "log-level",
      descr = "Parameter to change the application log level",
      required = false
    )
  }

  val generateState = new Subcommand("generate-state") {

    val tableExclude: ScallopOption[String] =
      opt[String]("table-exclude", descr = "Regex to exclude tables.", required = false)

    val tableInclude: ScallopOption[String] =
      opt[String]("table-include", descr = "Regex to include tables.", required = false)

    val columnInclude: ScallopOption[String] =
      opt[String]("column-include", descr = "Regex to include columns.", required = false)

    val columnExclude: ScallopOption[String] =
      opt[String]("column-exclude", descr = "Regex to exclude columns.", required = false)

    val tableNameRemove: ScallopOption[String] = opt[String](
      "table-name-remove",
      descr = "Removes a given regex from table name.",
      required = false
    )

    val columnNameRemove: ScallopOption[String] = opt[String](
      "column-name-remove",
      descr = "Removes a given regex from column name.",
      required = false
    )

    val outputPath: ScallopOption[String] =
      opt[String](
        "output-path",
        descr = "Directory to save the generated state file per table.",
        required = true
      )

    val sourceStateDirectory: ScallopOption[String] = opt[String](
      "source-state-directory",
      descr = "Source schema state directory generated through streamliner.",
      required = true
    )

    val tableCsvPath: ScallopOption[String] =
      opt[String]("table-csv-file", descr = "Tables CSV file path.", required = true)

    val columnCsvPath: ScallopOption[String] =
      opt[String]("column-csv-file", descr = "Columns CSV file path.", required = true)
  }

  addSubcommand(schema)
  addSubcommand(produceScripts)
  addSubcommand(generateState)

  verify()
}
