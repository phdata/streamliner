package io.phdata.streamliner.configuration.mapper

import com.amazonaws.services.glue.model.{Column => AWSColumn}
import com.amazonaws.services.glue.model.{Table => AWSTable}
import io.phdata.streamliner.util.TemplateFunction
import io.phdata.streamliner.configuration._
import schemacrawler.schema.{Column => SchemaCrawlerColumn}
import schemacrawler.schema.{Table => SchemaCrawlerTable}

import collection.JavaConverters._

object SnowflakeTableMapper {

  def mapSchemaCrawlerTables(
      tables: List[SchemaCrawlerTable],
      userDefinedTables: Option[Seq[UserDefinedTable]]): Seq[TableDefinition] =
    userTableDefinitions(tables.map(mapSchemaCrawlerTable), userDefinedTables)

  def mapAWSGlueTables(
      tables: List[AWSTable],
      userDefinedTables: Option[Seq[UserDefinedTable]]): Seq[TableDefinition] =
    userTableDefinitions(tables.map(mapAWSGlueTable), userDefinedTables)

  def mapSchemaCrawlerTable(table: SchemaCrawlerTable): SnowflakeTable = {
    SnowflakeTable(
      `type` = "Snowflake",
      sourceName = table.getName,
      destinationName = TemplateFunction.cleanse(table.getName),
      comment = Option(table.getRemarks),
      primaryKeys = table.getColumns.asScala.filter(c => c.isPartOfPrimaryKey).map(_.getName),
      changeColumn = None,
      incrementalTimeStamp = None,
      metadata = None,
      fileFormat = FileFormat(
        name = "",
        options = Map()
      ),
      columns = table.getColumns.asScala.map(mapSchemaCrawlerColumnDefinition)
    )
  }

  def mapAWSGlueTable(table: AWSTable): SnowflakeTable = {
    SnowflakeTable(
      `type` = "Snowflake",
      sourceName = table.getName,
      destinationName = TemplateFunction.cleanse(table.getName),
      comment = Option(table.getDescription),
      primaryKeys = Seq(),
      changeColumn = None,
      incrementalTimeStamp = None,
      metadata = None,
      fileFormat = FileFormat(
        name = "",
        options = Map(
          "TYPE" -> table.getStorageDescriptor.getOutputFormat
        )
      ),
      columns = table.getStorageDescriptor.getColumns.asScala.map(mapAWSGlueColumnDefinition)
    )
  }

  private def mapSchemaCrawlerColumnDefinition(column: SchemaCrawlerColumn): ColumnDefinition =
    ColumnDefinition(
      sourceName = column.getName,
      destinationName = TemplateFunction.cleanse(column.getName),
      dataType = column.getColumnDataType.getName,
      comment = Option(column.getRemarks),
      precision = Option(column.getSize),
      scale = Option(column.getDecimalDigits)
    )

  private def mapAWSGlueColumnDefinition(column: AWSColumn): ColumnDefinition =
    ColumnDefinition(
      sourceName = column.getName,
      destinationName = TemplateFunction.cleanse(column.getName),
      dataType = column.getType,
      comment = Option(column.getComment)
    )

  private def userTableDefinitions(
      tables: Seq[SnowflakeTable],
      userDefinedTablesOpt: Option[Seq[UserDefinedTable]]): Seq[SnowflakeTable] = {
    userDefinedTablesOpt match {
      case Some(userDefinedTables) =>
        tables.map { table =>
          val userTables = userDefinedTables.map(ut => ut.asInstanceOf[SnowflakeUserDefinedTable])
          userTables.find(ut => ut.name.equalsIgnoreCase(table.sourceName)) match {
            case Some(userTable) =>
              val fileFormat = userTable.fileFormat match {
                case Some(fileFormat) =>
                  FileFormat(name = fileFormat.name, options = fileFormat.options)
                case None => table.fileFormat
              }
              table.copy(
                primaryKeys = userTable.primaryKeys.getOrElse(table.primaryKeys),
                fileFormat = fileFormat
              )
            case None => table
          }
        }
      case None => tables
    }
  }
}
