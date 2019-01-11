package io.phdata.pipewrench

import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

class Cli(args: Seq[String]) extends ScallopConf(args) {

  val configuration = new Subcommand("configuration") {
    val filePath: ScallopOption[String] = opt[String](
      "configuration",
      'c',
      descr = "Path to ingest configuration",
      required = true)

    val outputPath: ScallopOption[String] = opt[String](
      "output-path",
      'o',
      descr = "Directory path where Pipewrench configuration should be written to",
      required = false)
  }

  val produceScripts = new Subcommand("scripts") {
    val filePath: ScallopOption[String] = opt[String](
      "configuration",
      'c',
      descr = "Path to pipewrench configuration",
      required = true)
    val outputPath: ScallopOption[String] = opt[String](
      "output-path",
      'o',
      descr = "Directory path where scripts should be written to",
      required = false)
    val typeMappingFile: ScallopOption[String] = opt[String](
      "type-mapping",
      'm',
      descr = "Path to data type mapping file",
      required = false)
    val templateDirectory: ScallopOption[String] = opt[String](
      "template-directory",
      't',
      descr = "Template directory path",
      required = false
    )
  }

  addSubcommand(configuration)
  addSubcommand(produceScripts)

  verify()
}
