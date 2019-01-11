package io.phdata.pipewrench.util

import io.phdata.pipewrench.configuration.{ColumnDefinition, Configuration, TableDefinition, TypeMapping}

object TemplateFunction {

  def mapDataType(column: ColumnDefinition, typeMapping: TypeMapping, storageFormat: String): String = {
    val cColumn = checkOracleNumberType(column)
    typeMapping.dataTypes.get(cColumn.dataType.toLowerCase) match {
      case Some(dataTypeMap) =>
        dataTypeMap.get(storageFormat.toLowerCase) match {
          case Some(dataType) => dataType
          case None => throw new RuntimeException(s"No type mapping found for data type: ${column.dataType} and storage format: $storageFormat in provided type mapping")
        }
      case None => throw new RuntimeException(s"No type mapping found for data type: ${column.dataType} in provided type mapping")
    }
  }

  def sourceColumns(configuration: Configuration, table: TableDefinition): String =
    table.columns.map {
      column =>
        val f = if (configuration.jdbc.driverClass.get.contains("oracle")) {
          column.sourceName + " AS " + "\"" + column.destinationName + "\""
        } else {
          "\"" + column.sourceName + "\" AS " + column.destinationName
        }
        f
    }.mkString(",\n")

  def columnOrCast(configuration: Configuration,
                   table: TableDefinition,
                   typeMapping: TypeMapping,
                   sourceFormat: String,
                   targetFormat: String): String =
    table.columns.map {
      column =>
        val sourceDataType = mapDataType(column, typeMapping, sourceFormat)
        val targetDataType = mapDataType(column, typeMapping, targetFormat)
        if (sourceDataType.equals(targetDataType)) {
          column.destinationName
        } else {
          if (sourceDataType.equalsIgnoreCase("STRING") && targetDataType.equalsIgnoreCase("DECIMAL")) {
            s"CAST(`${column.destinationName}` AS DECIMAL(${column.precision.get}, ${column.scale.get}) AS `${column.destinationName}`"
          } else {
            s"CAST(`${column.destinationName}` AS $targetDataType) AS `${column.destinationName}`"
          }
        }
    }.mkString(",\n")

  def primaryKeys(table: TableDefinition): String = table.primaryKeys.map(pk => s"`$pk`").mkString(",")

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
