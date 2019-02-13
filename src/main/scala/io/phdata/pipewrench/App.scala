package io.phdata.pipewrench

import java.io.FileNotFoundException

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
        val enhancedConfiguration = ConfigurationBuilder.build(configuration)
        enhancedConfiguration.writeYamlFile(s"$outputDirectory/pipewrench-configuration.yml")
        if (cli.configuration.createDocs()) {
          SchemaCrawlerImpl
            .getErdOutput(enhancedConfiguration.jdbc, outputDirectory)
          SchemaCrawlerImpl
            .getHtmlOutput(enhancedConfiguration.jdbc, outputDirectory)
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
        logger.error("Please provide a valid sub command options are `configuration` and `scripts`")

    }
  }
}
