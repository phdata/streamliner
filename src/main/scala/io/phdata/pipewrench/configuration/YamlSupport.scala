package io.phdata.pipewrench.configuration

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._
import io.phdata.pipewrench.util.FileUtil

trait YamlSupport extends DefaultYamlProtocol with FileUtil {

  implicit def typeMappingYamlFormat = yamlFormat1(TypeMapping.apply)
  implicit def columnDefinitionYamlFormat = yamlFormat6(ColumnDefinition.apply)
  implicit def tableDefinitionYamlFormat = yamlFormat10(TableDefinition.apply)
  implicit def tableYamlFormat = yamlFormat6(Table.apply)
  implicit def databaseYamlFormat = yamlFormat2(Database.apply)
  implicit def hadoopYamlFormat = yamlFormat5(Hadoop.apply)
  implicit def jdbcYamlFormat = yamlFormat7(Jdbc.apply)
  implicit def configurationYamlFormat = yamlFormat5(Configuration.apply)
  implicit def pipewrenchConfigurationYamlFormat = yamlFormat2(PipewrenchConfiguration.apply)

  def readConfigurationFile(path: String): Configuration = readFile(path).parseYaml.convertTo[Configuration]
  def readPipewrenchConfigurationFile(path: String): PipewrenchConfiguration = readFile(path).parseYaml.convertTo[PipewrenchConfiguration]
  def readTypeMappingFile(path: String): TypeMapping = readFile(path).parseYaml.convertTo[TypeMapping]

  implicit class WritePipewrenchConfigurationYamlFile(configuration: PipewrenchConfiguration) {
    def writeYamlFile(path: String): Unit = writeFile(configuration.toYaml.prettyPrint, path)
  }

}
