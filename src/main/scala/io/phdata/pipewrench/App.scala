package io.phdata.pipewrench

import com.typesafe.scalalogging.LazyLogging
import io.phdata.pipewrench.configuration.ConfigurationBuilder
import io.phdata.pipewrench.configuration.Default
import io.phdata.pipewrench.configuration.YamlSupport
import io.phdata.pipewrench.pipeline.PipelineBuilder
import io.phdata.pipewrench.schemacrawler.SchemaCrawlerImpl
import io.phdata.pipewrench.util.FileUtil

object App extends YamlSupport with Default with FileUtil with LazyLogging {

  def main(args: Array[String]): Unit = {
    val cli = new Cli(args)

    cli.subcommand match {
      case Some(cli.configuration) =>
        val configuration = readConfigurationFile(cli.configuration.filePath())

        val outputDirectory =
          configurationOutputDirectory(configuration, cli.configuration.outputPath.toOption)
        createDir(outputDirectory)
        val pipewrenchConfiguration = ConfigurationBuilder.build(configuration)
        pipewrenchConfiguration.writeYamlFile(s"$outputDirectory/pipewrench-configuration.yml")
        SchemaCrawlerImpl.getErdOutput(pipewrenchConfiguration.configuration.jdbc, outputDirectory)
        SchemaCrawlerImpl.getHtmlOutput(pipewrenchConfiguration.configuration.jdbc, outputDirectory)

      case Some(cli.produceScripts) =>
        val configuration = readPipewrenchConfigurationFile(cli.produceScripts.filePath())
        val typeMapping = readTypeMappingFile(
          cli.produceScripts.typeMappingFile.getOrElse(TYPE_MAPPING_FILE))
        createDir(cli.produceScripts.outputPath.getOrElse(OUTPUT_DIRECTORY))
        PipelineBuilder.build(
          configuration,
          typeMapping,
          cli.produceScripts.templateDirectory.getOrElse(TEMPLATE_DIRECTORY),
          cli.produceScripts.outputPath.getOrElse(OUTPUT_DIRECTORY)
        )

      case None =>
        logger.error("Please provide a valid sub command options are `configuration` and `scripts`")

    }
  }
}
