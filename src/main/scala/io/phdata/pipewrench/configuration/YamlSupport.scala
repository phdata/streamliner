package io.phdata.pipewrench.configuration

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._
import io.phdata.pipewrench.util.FileUtil

trait YamlSupport extends DefaultYamlProtocol with FileUtil {

  implicit def columnDefinitionYamlFormat = yamlFormat6(ColumnDefinition.apply)
  implicit def tableDefinitionYamlFormat = yamlFormat10(TableDefinition.apply)
  implicit def tableYamlFormat = yamlFormat6(Table.apply)
  implicit def databaseYamlFormat = yamlFormat2(Database.apply)
  implicit def hadoopYamlFormat = yamlFormat5(Hadoop.apply)
  implicit def jdbcYamlFormat = yamlFormat7(Jdbc.apply)
  implicit def configurationYamlFormat = yamlFormat6(Configuration.apply)

  def readConfigurationFile(path: String): Configuration =
    readFile(path).parseYaml.convertTo[Configuration]

  def readTypeMappingFile(path: String): Map[String, Map[String, String]] =
    readFile(path).parseYaml.convertTo[Map[String, Map[String, String]]]

  implicit class WritePipewrenchConfigurationYamlFile(configuration: Configuration) {
    def writeYamlFile(path: String): Unit = writeFile(configuration.toYaml.prettyPrint, path)
  }

}
