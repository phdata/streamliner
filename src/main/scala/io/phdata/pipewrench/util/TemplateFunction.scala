package io.phdata.pipewrench.util

import io.phdata.pipewrench.configuration.ColumnDefinition
import io.phdata.pipewrench.configuration.Configuration
import io.phdata.pipewrench.configuration.TableDefinition

object TemplateFunction {

  def mapDataType(
      column: ColumnDefinition,
      typeMapping: Map[String, Map[String, String]],
      storageFormat: String): String = {
    val cColumn = checkOracleNumberType(column)
    typeMapping.get(cColumn.dataType.toLowerCase) match {
      case Some(dataTypeMap) =>
        dataTypeMap.get(storageFormat.toLowerCase) match {
          case Some(dataType) => dataType
          case None =>
            throw new RuntimeException(
              s"No type mapping found for data type: ${column.dataType} and storage format: $storageFormat in provided type mapping")
        }
      case None =>
        throw new RuntimeException(
          s"No type mapping found for data type: ${column.dataType} in provided type mapping")
    }
  }

  def sqoopMapJavaColumn(tableDefinition: TableDefinition): Option[String] = {
    val map = tableDefinition.columns.flatMap { column =>
      val stringTypes =
        Seq("clob", "longvarbinary", "varbinary", "rowid", "blob", "nclob", "text", "binary")
      val intTypes = Seq("tinyint", "int", "smallint", "integer", "short")
      val floatTypes = Seq("float")
      val longDataTypes = Seq("bigint")
      if (stringTypes.contains(column.dataType.toLowerCase)) {
        Some(s"${column.destinationName}=String")
      } else if (floatTypes.contains(column.dataType.toLowerCase)) {
        Some(s"${column.destinationName}=Float")
      } else if (intTypes.contains(column.dataType.toLowerCase)) {
        Some(s"${column.destinationName}=Integer")
      } else if (longDataTypes.contains(column.dataType.toLowerCase)) {
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

  def sourceColumns(configuration: Configuration, table: TableDefinition): String =
    table.columns
      .map { column =>
        val f = if (configuration.jdbc.driverClass.get.contains("oracle")) {
          column.sourceName + " AS " + "\"" + column.destinationName + "\""
        } else {
          "\"" + column.sourceName + "\" AS " + column.destinationName
        }
        f
      }
      .mkString(",\n")

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
          column.destinationName
        } else {
          if (sourceDataType.equalsIgnoreCase("STRING") && targetDataType.equalsIgnoreCase(
              "DECIMAL")) {
            s"CAST(`${column.destinationName}` AS DECIMAL(${column.precision.get}, ${column.scale.get}) AS `${column.destinationName}`"
          } else {
            s"CAST(`${column.destinationName}` AS $targetDataType) AS `${column.destinationName}`"
          }
        }
      }
      .mkString(",\n")

  def primaryKeys(table: TableDefinition): String =
    table.primaryKeys.map(pk => s"`$pk`").mkString(",")

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
}
