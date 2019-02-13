package io.phdata.pipewrench

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.Subcommand

class Cli(args: Seq[String]) extends ScallopConf(args) {

  val configuration = new Subcommand("configuration") {

    val filePath: ScallopOption[String] =
      opt[String]("configuration", descr = "Path to ingest configuration", required = true)

    val outputPath: ScallopOption[String] = opt[String](
      "output-path",
      descr = "Directory path where Pipewrench configuration should be written to",
      required = false)

    val createDocs: ScallopOption[Boolean] = opt[Boolean](
      "create-docs",
      descr = "Flag to indicate whether the HTML and ERD documentation should be produced",
      required = false,
      default = Some(false)
    )
  }

  val produceScripts = new Subcommand("scripts") {

    val filePath: ScallopOption[String] =
      opt[String]("configuration", descr = "Path to pipewrench configuration", required = true)

    val outputPath: ScallopOption[String] = opt[String](
      "output-path",
      descr = "Directory path where scripts should be written to",
      required = false)

    val typeMappingFile: ScallopOption[String] =
      opt[String]("type-mapping", descr = "Path to data type mapping file", required = false)

    val templateDirectory: ScallopOption[String] = opt[String](
      "template-directory",
      descr = "Template directory path",
      required = true
    )
  }

  addSubcommand(configuration)
  addSubcommand(produceScripts)

  verify()
}
