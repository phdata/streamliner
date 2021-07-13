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

    val filePath: ScallopOption[String] =
      opt[String]("config", descr = "Path to ingest configuration", required = true)

    val outputFile: ScallopOption[String] = opt[String](
      "output-file",
      descr = "File name with path where Streamliner configuration should be written to",
      required = true)

    val previousOutputFile: ScallopOption[String] = opt[String](
      "previous-output-file",
      descr = "Previous run file name with path where Streamliner configuration is written to",
      required = false)

    val diffOutputFile: ScallopOption[String] =
      opt[String](
        "diff-output-file",
        descr =
          "File name with path where Streamliner configuration difference should be  written to",
        required = false)

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
  }

  val produceScripts = new Subcommand("scripts") {

    val filePath: ScallopOption[String] =
      opt[String]("config", descr = "Path to streamliner configuration", required = true)

    val configDiffFilePath: ScallopOption[String] =
      opt[String](
        "config-diff",
        descr = "Path to streamliner configuration difference",
        required = false)

    val outputPath: ScallopOption[String] = opt[String](
      "output-path",
      descr = "Directory path where scripts should be written to",
      required = false)

    val typeMappingFile: ScallopOption[String] =
      opt[String]("type-mapping", descr = "Path to data type mapping file", required = true)

    val templateDirectory: ScallopOption[String] = opt[String](
      "template-directory",
      descr = "Template directory path",
      required = true
    )
  }

  addSubcommand(schema)
  addSubcommand(produceScripts)

  verify()
}
