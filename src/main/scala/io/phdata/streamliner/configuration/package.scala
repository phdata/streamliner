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

package io.phdata.streamliner

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

package object configuration {

  case class Configuration(
      name: String,
      environment: String,
      pipeline: String,
      source: Source,
      destination: Destination,
      tables: Option[Seq[TableDefinition]] = None)

  sealed trait Source

  object Source {
    implicit val decodeSource: Decoder[Source] =
      Decoder[GlueCatalog].map[Source](identity)
      .or(Decoder[Jdbc].map[Source](identity))

    implicit val encodeSource: Encoder[Source] = Encoder.instance {
      case glue @ GlueCatalog(_, _) => glue.asJson
      case jdbc @ Jdbc(_, _, _, _, _, _, _, _, _, _) => jdbc.asJson
    }
  }

  case class GlueCatalog(region: String, database: String) extends Source

  case class Jdbc(
      driverClass: Option[String],
      url: String,
      username: String,
      passwordFile: String,
      jceKeyStorePath: Option[String],
      keystoreAlias: Option[String],
      schema: String,
      tableTypes: Seq[String],
      tables: Option[Seq[Table]],
      metadata: Option[Map[String, String]])
      extends Source

  sealed trait Destination

  object Destination {
    implicit val decodeDestination: Decoder[Destination] =
      Decoder[Hadoop]
        .map[Destination](identity)
        .or(Decoder[Snowflake].map[Destination](identity))

    implicit val encodeData: Encoder[Destination] = Encoder.instance {
      case hadoop @ Hadoop(_, _, _) => hadoop.asJson
      case snowflake @ Snowflake(_, _, _, _, _, _, _) => snowflake.asJson
    }
  }

  case class Hadoop(
      impalaShellCommand: String,
      stagingDatabase: HadoopDatabase,
      reportingDatabase: HadoopDatabase)
      extends Destination

  case class Snowflake(
      snowSqlCommand: String,
      storagePath: String,
      storageIntegration: String,
      warehouse: String,
      taskSchedule: String,
      stagingDatabase: SnowflakeDatabase,
      reportingDatabase: SnowflakeDatabase)
      extends Destination

  case class HadoopDatabase(name: String, path: String)
  case class SnowflakeDatabase(name: String, schema: String)

  case class Table(
      name: String,
      checkColumn: Option[String] = None,
      numberOfMappers: Option[Int] = Some(1),
      splitByColumn: Option[String] = None,
      numberOfPartitions: Option[Int] = Some(2),
      metadata: Option[Map[String, String]])

  case class TableDefinition(
      sourceName: String,
      destinationName: String,
      checkColumn: Option[String] = None,
      comment: Option[String],
      primaryKeys: Seq[String],
      metadata: Option[Map[String, String]] = None,
      numberOfMappers: Option[Int] = None,
      splitByColumn: Option[String] = None,
      numberOfPartitions: Option[Int] = None,
      storage: Option[StorageDefinition] = None,
      columns: Seq[ColumnDefinition])

  case class ColumnDefinition(
      sourceName: String,
      destinationName: String,
      dataType: String,
      comment: Option[String] = None,
      precision: Option[Int] = None,
      scale: Option[Int] = None)

  case class StorageDefinition(
      location: String,
      fileType: String,
                              )
}
