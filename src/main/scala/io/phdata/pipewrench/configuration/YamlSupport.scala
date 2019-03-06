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
