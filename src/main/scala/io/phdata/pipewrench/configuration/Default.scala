package io.phdata.pipewrench.configuration

trait Default {

  lazy val TYPE_MAPPING_FILE = "src/main/resources/type-mapping.yml"
  lazy val TEMPLATE_DIRECTORY = "src/main/resources/templates"
  lazy val OUTPUT_DIRECTORY = "output"

  def scriptsDirectory(configuration: Configuration, outputDirectory: String): String =
    s"${dir(configuration, outputDirectory)}/scripts"

  def configurationOutputDirectory(
      configuration: Configuration,
      outputDirectory: Option[String]): String =
    dir(configuration, outputDirectory.getOrElse(OUTPUT_DIRECTORY))

  private def dir(configuration: Configuration, path: String): String =
    s"$path/${configuration.name}/${configuration.environment}"

}
