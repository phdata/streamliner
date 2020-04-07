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

package io.phdata.streamliner.configuration
import java.io.FileNotFoundException

import io.circe.generic.AutoDerivation
import io.circe.yaml.Printer
import io.circe.yaml.parser
import io.phdata.streamliner.util.FileUtil
import io.circe.syntax._

trait YamlSupport extends FileUtil with AutoDerivation {

  type TypeMapping = Map[String, Map[String, String]]

  def readConfigurationFile(path: String): Configuration = {
    if (!fileExists(path)) {
      throw new FileNotFoundException(s"Configuration file not found: '$path'.")
    }

    parser.parse(readFile(path)) match {
      case Right(v) =>
        v.as[Configuration] match {
          case Right(c) => c
          case Left(err) => throw err
        }
      case Left(err) => throw err
    }
  }

  def readTypeMappingFile(path: String): TypeMapping = {
    if (!fileExists(path)) {
      throw new FileNotFoundException(s"Type mapping file not found: '$path'.")
    }

    parser.parse(readFile(path)) match {
      case Right(v) =>
        v.as[TypeMapping] match {
          case Right(c) => c
          case Left(err) => throw err
        }
      case Left(err) => throw err
    }
  }

  def writeConfiguration(configuration: Configuration, path: String): Unit = {
    createDir(path)

    val printed = Printer(
      preserveOrder = true,
      dropNullKeys = true,
      mappingStyle = Printer.FlowStyle.Block
    ).pretty(configuration.asJson)

    writeFile(printed, s"$path/pipewrench-configuration.yml")
  }
}
