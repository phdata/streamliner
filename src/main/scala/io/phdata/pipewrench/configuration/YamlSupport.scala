package io.phdata.pipewrench.configuration
import io.circe.generic.AutoDerivation
import io.circe.yaml.Printer
import io.circe.yaml.parser
import io.phdata.pipewrench.util.FileUtil
import io.circe.syntax._

trait YamlSupport extends FileUtil with AutoDerivation {

  type TypeMapping = Map[String, Map[String, String]]

  def readConfigurationFile(path: String): Configuration = parseConfiguration(readFile(path))

  protected def parseConfiguration(yaml: String): Configuration = parser.parse(yaml) match {
    case Right(v) =>
      v.as[Configuration] match {
        case Right(c) => c
        case Left(err) => throw err

      }
    case Left(err) => throw err
  }

  def readTypeMappingFile(path: String): TypeMapping = parseTypeMapping(readFile(path))

  protected def parseTypeMapping(yaml: String) = parser.parse(yaml) match {
    case Right(v) =>
      v.as[TypeMapping] match {
        case Right(c) => c
        case Left(err) => throw err
      }
    case Left(err) => throw err
  }

  protected def prettyPrintConfiguration(configuration: Configuration): String = {
    val json = configuration.asJson
    Printer(dropNullKeys = true, mappingStyle = Printer.FlowStyle.Block)
      .pretty(json)
  }

  def writeYamlFile(configuration: Configuration, path: String): Unit = {
    val printed = prettyPrintConfiguration(configuration)
    writeFile(printed, path)
  }

}
