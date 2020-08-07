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

package io.phdata.streamliner.util

import io.phdata.streamliner.configuration.ColumnDefinition
import io.phdata.streamliner.configuration.Configuration
import io.phdata.streamliner.configuration.HadoopTable
import io.phdata.streamliner.configuration.Jdbc
import io.phdata.streamliner.configuration.SnowflakeTable
import io.phdata.streamliner.configuration.TableDefinition

object TemplateFunction {

  def mapDataType(
      column: ColumnDefinition,
      typeMapping: Map[String, Map[String, String]],
      storageFormat: String): String = {
    val cColumn = normalizeColumnDefinition(column)
    typeMapping.get(cColumn.dataType.toLowerCase) match {
      case Some(dataTypeMap) =>
        dataTypeMap.get(storageFormat.toLowerCase) match {
          case Some(dataType) => dataType
          case None =>
            throw new RuntimeException(
              s"No type mapping found for data type: '${column.dataType}' and storage format: $storageFormat in provided type mapping")
        }
      case None =>
        throw new RuntimeException(
          s"No type mapping found for data type: '${column.dataType}' in provided type mapping")
    }
  }

  def sqoopMapJavaColumn(tableDefinition: HadoopTable): Option[String] = {
    val map = tableDefinition.columns.flatMap { column =>
      val stringTypes =
        Seq("clob", "longvarbinary", "varbinary", "rowid", "blob", "nclob", "text", "binary")
      val intTypes = Seq("tinyint", "int", "smallint", "integer", "short")
      val floatTypes = Seq("float")
      val longDataTypes = Seq("bigint")
      val dataType = checkOracleNumberType(column).dataType.toLowerCase
      if (stringTypes.contains(dataType)) {
        Some(s"${column.destinationName}=String")
      } else if (floatTypes.contains(dataType)) {
        Some(s"${column.destinationName}=Float")
      } else if (intTypes.contains(dataType)) {
        Some(s"${column.destinationName}=Integer")
      } else if (longDataTypes.contains(dataType)) {
        Some(s"${column.destinationName}=Long")
      } else {
        None
      }
    }
    if (map.isEmpty) {
      None
    } else {
      Some(map.mkString(","))
    }
  }

  def cleanse(s: String): String = {
    val specialCharRegex = "(/|-|\\(|\\)|\\s|\\$)".r
    val specialChars = specialCharRegex.replaceAllIn(s.toLowerCase, "_")
    val dupsRegex = "(_{2,})".r
    val dups = dupsRegex.replaceAllIn(specialChars, "_")

    if (dups.startsWith("/") || dups.startsWith("_")) {
      dups.substring(1, dups.length)
    } else {
      dups
    }
  }

  def sourceColumns(configuration: Configuration, table: TableDefinition): String = {
    val jdbc = configuration.source.asInstanceOf[Jdbc]
    table.columns
      .map { column =>
        val f =
          if (jdbc.driverClass.get.contains("oracle") || jdbc.driverClass.get.contains("sqlserver")) {
            column.sourceName + " AS " + "\"" + column.destinationName + "\""
          } else {
            "`" + column.sourceName + "` AS " + column.destinationName
          }
        f
      }
      .mkString(",\n")
  }

  def columnOrCast(
      configuration: Configuration,
      table: TableDefinition,
      typeMapping: Map[String, Map[String, String]],
      sourceFormat: String,
      targetFormat: String): String =
    table.columns
      .map { column =>
        val sourceDataType = mapDataType(column, typeMapping, sourceFormat)
        val targetDataType = mapDataType(column, typeMapping, targetFormat)
        if (sourceDataType.equals(targetDataType)) {
          s"`${column.destinationName}`"
        } else {
          if (sourceDataType.equalsIgnoreCase("STRING") && targetDataType.equalsIgnoreCase(
              "DECIMAL")) {
            s"CAST(`${column.destinationName}` AS DECIMAL(${column.precision.get}, ${column.scale.get})) AS `${column.destinationName}`"
          } else {
            s"CAST(`${column.destinationName}` AS $targetDataType) AS `${column.destinationName}`"
          }
        }
      }
      .mkString(",\n")

  def primaryKeys(table: TableDefinition): String =
    table.primaryKeys.map(pk => s"`$pk`").mkString(",")

  /**
   * Normalize database-specific column definition properties.
   * @param columnDefinition ColumnDefinition to normalize
   * @return a new ColumnDefinition if changes are needed, the original ColumnDefinition otherwise
   */
  private[util] def normalizeColumnDefinition(
      columnDefinition: ColumnDefinition): ColumnDefinition = {
    val col = columnDefinition.copy(
      dataType = columnDefinition.dataType.toLowerCase.stripSuffix(" identity"))
    checkOracleNumberType(col)
  }

  private def checkOracleNumberType(columnDefinition: ColumnDefinition): ColumnDefinition = {
    val precision = columnDefinition.precision.getOrElse(0)
    val scale = columnDefinition.scale.getOrElse(0)
    if (columnDefinition.dataType.toUpperCase.equals("NUMBER")) {
      if (scale > 0) {
        columnDefinition.copy(dataType = "DECIMAL")
      } else if (precision > 19 && scale == 0) {
        columnDefinition.copy(dataType = "DECIMAL")
      } else if (precision >= 10 && precision <= 19 && scale == 0) {
        columnDefinition.copy(dataType = "BIGINT")
      } else if (precision == 0 && scale == -127) {
        columnDefinition.copy(dataType = "VARCHAR")
      } else {
        columnDefinition.copy(dataType = "INTEGER")
      }
    } else {
      columnDefinition
    }
  }

  def orderColumns(table: TableDefinition): Seq[ColumnDefinition] = {
    val primaryKeys = table.primaryKeys
    val primaryKeyColumnDefs =
      table.columns.filter(column => primaryKeys.contains(column.sourceName))
    val nonPrimaryKeyColumnDefs =
      table.columns.filter(column => !primaryKeys.contains(column.sourceName))
    primaryKeyColumnDefs ++ nonPrimaryKeyColumnDefs
  }
}
