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

  // only add quotes if required since it makes the identifier case sensitive
  // note we call this for hadoop and snowflake, but hadoop cleanses columns
  // so in the hadoop case we'll always hit the else
  val IDENTIFIER_WITHOUT_SPECIAL_CHARS = "^[A-Za-z0-9_]+$";

  def quoteIdentifierIfNeeded(identifier: String): String = {
    if (identifier.matches(IDENTIFIER_WITHOUT_SPECIAL_CHARS)) {
      identifier
    } else {
      "\"" + identifier + "\""
    }
  }

  type TypeMapping = Map[String, Map[String, String]]

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
      quality: Option[SnowflakeQAOptions],
      stagingDatabase: SnowflakeDatabase,
      reportingDatabase: SnowflakeDatabase)
      extends Destination

  case class HadoopDatabase(name: String, path: String)

  case class SnowflakeDatabase(name: String, schema: String)

  case class SnowflakeQAOptions(
      taskSchedule: Option[String],
      minimumPercentage: Option[Double],
      minimumCount: Option[Int],
      minimumRuns: Option[Int],
      standardDeviations: Option[Double]
  )

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

    def columnList(aliasOpt: Option[String] = None): String =
      columns
        .map { column =>
          aliasOpt match {
            case Some(alias) => s"${alias}.${quoteIdentifierIfNeeded(column.destinationName)}"
            case None => s"${quoteIdentifierIfNeeded(column.destinationName)}"
          }
        }
        .mkString(",")

    def pkConstraint(aAlias: String, bAlias: String, joinCondition: String = " AND "): String =
      primaryKeys
        .map { pk =>
          s"${aAlias}.${pk} = ${bAlias}.${pk}"
        }
        .mkString(joinCondition)

    def columnConstraint(
        aAlias: Option[String] = None,
        bAlias: String,
        joinCondition: String = " AND "): String =
      columns
        .map { column =>
          aAlias match {
            case Some(alias) =>
              s"${alias}.${quoteIdentifierIfNeeded(column.destinationName)} = ${bAlias}.${quoteIdentifierIfNeeded(column.destinationName)}"
            case None => s"${column.destinationName} = ${bAlias}.${column.destinationName}"
          }
        }
        .mkString(joinCondition)
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
      extends TableDefinition {

    lazy val pkList: String = primaryKeys.map(pk => s"`$pk`").mkString(",")

    lazy val orderColumnsPKsFirst: Seq[ColumnDefinition] = {
      val pkColumnDefs = columns.filter(c => primaryKeys.contains(c.sourceName))
      val nonPKColumnDefs = columns.filterNot(c => primaryKeys.contains(c.sourceName))
      pkColumnDefs ++ nonPKColumnDefs
    }

    def columnDDL(typeMapping: TypeMapping, targetFormat: String): String =
      columns
        .map { column =>
          s"`${column.destinationName}` ${column.mapDataTypeHadoop(typeMapping, targetFormat)} COMMENT '${column.comment
            .getOrElse("")}'"
        }
        .mkString(",\n")

    def sourceColumns(driverClass: String): String =
      columns
        .map { column =>
          if (driverClass.toLowerCase.contains("oracle") || driverClass.toLowerCase.contains(
              "sqlserver")) {
            column.sourceName + " AS " + "\"" + column.destinationName + "\""
          } else {
            s"`${column.sourceName}` AS ${column.destinationName}"
          }
        }
        .mkString(",\n")

    def sqoopMapJavaColumns(typeMapping: TypeMapping): Option[String] = {
      val map = columns.flatMap(c => c.mapDataTypeJava(typeMapping))

      if (map.isEmpty) {
        None
      } else {
        Some(map.mkString(","))
      }
    }
  }

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
      extends TableDefinition {

    lazy val pkList: String = primaryKeys.mkString(",")

    def sourceColumnConversion(typeMapping: TypeMapping): String = {
      columns.map { column =>
        s"$$1:${quoteIdentifierIfNeeded(column.sourceName)}::${column.mapDataTypeSnowflake(typeMapping)}"
      }
    }.mkString(",\n")

    def columnDDL(typeMapping: TypeMapping): String = {
      columns.map { column =>
        s"${quoteIdentifierIfNeeded(column.destinationName)} ${column.mapDataTypeSnowflake(
          typeMapping)} COMMENT '${column.comment.getOrElse("")}'"
      }
    }.mkString(",\n")

  }

  case class ColumnDefinition(
      sourceName: String,
      destinationName: String,
      dataType: String,
      comment: Option[String] = None,
      precision: Option[Int] = None,
      scale: Option[Int] = None) {

    def cleanseDataType: String = dataType.toUpperCase.stripSuffix(" IDENTITY")

    def mapDataTypeHadoop(typeMapping: TypeMapping, targetFormat: String): String = {
      val cleanDataType = cleanseDataType
      val p = precision.getOrElse(0)
      val s = scale.getOrElse(0)

      // Oracle specific type mapping
      if (cleanDataType.equalsIgnoreCase("NUMBER")) {
        if (s > 0) {
          s"DECIMAL($p, $s)"
        } else if (p > 19 && s == 0) {
          s"DECIMAL($p, $s)"
        } else if (p >= 10 && p <= 19 && s == 0) {
          s"BIGINT"
        } else if (p == 0 && s == -127) {
          s"VARCHAR"
        } else {
          s"INTEGER"
        }
      } else if (cleanDataType.equalsIgnoreCase("DECIMAL")) {
        s"DECIMAL($p, $s)"
      } else {
        mapDataType(cleanDataType, typeMapping, targetFormat)
      }
    }

    def mapDataTypeSnowflake(typeMapping: TypeMapping): String = {
      val cleanDataType = cleanseDataType
      val p = precision.getOrElse(0)
      val s = scale.getOrElse(0)

      // Oracle specific type mapping
      if (cleanDataType.equalsIgnoreCase("NUMBER")) {
        if (p == 0 && (s == -127 || s == 0)) {
          s"NUMBER(38, 8)"
        } else {
          s"NUMBER($p, $s)"
        }
      } else {
        val dataType = mapDataType(cleanDataType, typeMapping, "SNOWFLAKE")
        if (dataType.equalsIgnoreCase("varchar")) {
          s"VARCHAR($p)"
        } else if (dataType.equalsIgnoreCase("char")) {
          s"CHAR($p)"
        } else {
          dataType
        }
      }
    }

    private def mapDataType(
        sourceDataType: String,
        typeMapping: TypeMapping,
        targetFormat: String): String = {
      typeMapping.get(sourceDataType.toLowerCase) match {
        case Some(dataTypeMap) =>
          dataTypeMap.get(targetFormat.toLowerCase) match {
            case Some(dataType) => dataType
            case None =>
              throw new RuntimeException(
                s"No type mapping found for data type: '$dataType' and storage format: $targetFormat in provided type mapping")
          }
        case None =>
          throw new RuntimeException(
            s"No type mapping found for data type: '$dataType' in provided type mapping file")
      }
    }

    def mapDataTypeJava(typeMapping: TypeMapping): Option[String] = {
      val strings =
        Seq("clob", "longvarbinary", "varbinary", "rowid", "blob", "nclob", "text", "binary")
      val ints = Seq("tinyint", "int", "smallint", "integer", "short")

      val dataType = mapDataTypeHadoop(typeMapping, "AVRO").toLowerCase
      if (strings.contains(dataType)) {
        Some(s"$destinationName=String")
      } else if (ints.contains(dataType)) {
        Some(s"$destinationName=Integer")
      } else if (dataType.equalsIgnoreCase("float")) {
        Some(s"$destinationName=Float")
      } else if (dataType.equalsIgnoreCase("bigint")) {
        Some(s"$destinationName=Long")
      } else {
        None
      }
    }

    def castColumn(typeMapping: TypeMapping, sourceFormat: String, targetFormat: String): String = {
      val sourceDataType = mapDataTypeHadoop(typeMapping, sourceFormat)
      val targetDataType = mapDataTypeHadoop(typeMapping, targetFormat)

      if (sourceDataType.equalsIgnoreCase(targetDataType)) {
        s"`${destinationName}`"
      } else {
        s"CAST(`$destinationName` AS $targetDataType) AS `${destinationName}`"
      }
    }
  }

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
      case snowflake @ Snowflake(_, _, _, _, _, _, _, _, _, _) => snowflake.asJson
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
