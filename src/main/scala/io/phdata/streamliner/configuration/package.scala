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

  sealed trait Source {
    val `type`: String
  }

  case class GlueCatalog(
      `type`: String,
      region: String,
      database: String,
      userDefinedTable: Option[Seq[UserDefinedTable]])
      extends Source

  case class Jdbc(
      `type`: String,
      driverClass: Option[String],
      url: String,
      username: String,
      passwordFile: String,
      jceKeyStorePath: Option[String],
      keystoreAlias: Option[String],
      schema: String,
      tableTypes: Seq[String],
      userDefinedTable: Option[Seq[UserDefinedTable]],
      metadata: Option[Map[String, String]])
      extends Source

  sealed trait Destination {
    val `type`: String
  }

  case class Hadoop(
      `type`: String,
      impalaShellCommand: String,
      stagingDatabase: HadoopDatabase,
      reportingDatabase: HadoopDatabase)
      extends Destination

  case class Snowflake(
      `type`: String,
      snowSqlCommand: String,
      storagePath: String,
      storageIntegration: String,
      snsTopic: Option[String],
      warehouse: String,
      taskSchedule: Option[String],
      stagingDatabase: SnowflakeDatabase,
      reportingDatabase: SnowflakeDatabase)
      extends Destination

  case class HadoopDatabase(name: String, path: String)
  case class SnowflakeDatabase(name: String, schema: String)

  sealed trait UserDefinedTable {
    val `type`: String
    val name: String
    val primaryKeys: Option[Seq[String]]
  }

  case class HadoopUserDefinedTable(
      `type`: String,
      name: String,
      primaryKeys: Option[Seq[String]],
      checkColumn: Option[String],
      numberOfMappers: Option[Int],
      splitByColumn: Option[String],
      numberOfPartitions: Option[Int],
      metadata: Option[Map[String, String]])
      extends UserDefinedTable

  case class SnowflakeUserDefinedTable(
      `type`: String,
      name: String,
      primaryKeys: Option[Seq[String]],
      fileFormat: Option[FileFormat])
      extends UserDefinedTable

  sealed trait TableDefinition {
    val `type`: String
    val sourceName: String
    val destinationName: String
    val primaryKeys: Seq[String]
    val columns: Seq[ColumnDefinition]
  }

  case class HadoopTable(
      `type`: String,
      sourceName: String,
      destinationName: String,
      checkColumn: Option[String],
      comment: Option[String],
      primaryKeys: Seq[String],
      metadata: Option[Map[String, String]],
      numberOfMappers: Option[Int],
      splitByColumn: Option[String],
      numberOfPartitions: Option[Int],
      columns: Seq[ColumnDefinition])
      extends TableDefinition

  case class SnowflakeTable(
      `type`: String,
      sourceName: String,
      destinationName: String,
      comment: Option[String],
      primaryKeys: Seq[String],
      changeColumn: Option[String],
      incrementalTimeStamp: Option[String],
      metadata: Option[Map[String, String]],
      fileFormat: FileFormat,
      columns: Seq[ColumnDefinition])
      extends TableDefinition

  case class ColumnDefinition(
      sourceName: String,
      destinationName: String,
      dataType: String,
      comment: Option[String] = None,
      precision: Option[Int] = None,
      scale: Option[Int] = None)

  case class FileFormat(
      location: String,
      fileType: String,
      delimiter: Option[String] = None,
      nullIf: Option[Set[String]] = None)

  object TableDefinition {
    implicit val decodeTableDefinition: Decoder[TableDefinition] = Decoder.instance(c =>
      c.downField("type").as[String].flatMap {
        case "Hadoop" => c.as[HadoopTable]
        case "Snowflake" => c.as[SnowflakeTable]
    })

    implicit val encodeTableDefinition: Encoder[TableDefinition] = Encoder.instance {
      case hadoopTable @ HadoopTable(_, _, _, _, _, _, _, _, _, _, _) => hadoopTable.asJson
      case snowflakeTable @ SnowflakeTable(_, _, _, _, _, _, _, _, _, _) => snowflakeTable.asJson
    }
  }

  object UserDefinedTable {
    implicit val decodeUserDefinedTable: Decoder[UserDefinedTable] = Decoder.instance(c =>
      c.downField("type").as[String].flatMap {
        case "Hadoop" => c.as[HadoopUserDefinedTable]
        case "Snowflake" => c.as[SnowflakeUserDefinedTable]
    })

    implicit val encodeUserDefinedTable: Encoder[UserDefinedTable] = Encoder.instance {
      case hadoopUserDefinedTable @ HadoopUserDefinedTable(_, _, _, _, _, _, _, _) =>
        hadoopUserDefinedTable.asJson
      case snowflakeUserDefinedTable @ SnowflakeUserDefinedTable(_, _, _, _) =>
        snowflakeUserDefinedTable.asJson
    }
  }

  object Destination {
    implicit val decodeDestination: Decoder[Destination] = Decoder.instance(c =>
      c.downField("type").as[String].flatMap {
        case "Hadoop" => c.as[Hadoop]
        case "Snowflake" => c.as[Snowflake]
    })

    implicit val encodeDestination: Encoder[Destination] = Encoder.instance {
      case hadoop @ Hadoop(_, _, _, _) => hadoop.asJson
      case snowflake @ Snowflake(_, _, _, _, _, _, _, _, _) => snowflake.asJson
    }
  }

  object Source {
    implicit val decodeSource: Decoder[Source] = Decoder.instance(c =>
      c.downField("type").as[String].flatMap {
        case "Glue" => c.as[GlueCatalog]
        case "Jdbc" => c.as[Jdbc]
    })

    implicit val encodeSource: Encoder[Source] = Encoder.instance {
      case glue @ GlueCatalog(_, _, _, _) => glue.asJson
      case jdbc @ Jdbc(_, _, _, _, _, _, _, _, _, _, _) => jdbc.asJson
    }
  }
}
