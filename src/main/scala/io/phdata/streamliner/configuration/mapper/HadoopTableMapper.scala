package io.phdata.streamliner.configuration.mapper

import com.amazonaws.services.glue.model.{Column => AWSColumn}
import com.amazonaws.services.glue.model.{Table => AWSTable}
import io.phdata.streamliner.configuration._
import schemacrawler.schema.{Column => SchemaCrawlerColumn}
import schemacrawler.schema.{Table => SchemaCrawlerTable}

import collection.JavaConverters._

object HadoopTableMapper extends TableMapper {

  def mapSchemaCrawlerTables(
      tables: List[SchemaCrawlerTable],
      userDefinedTables: Option[Seq[UserDefinedTable]]): Seq[TableDefinition] =
    userTableDefinitions(tables.map(mapSchemaCrawlerTable), userDefinedTables)

  def mapAWSGlueTables(
      tables: List[AWSTable],
      userDefinedTable: Option[Seq[UserDefinedTable]]): Seq[TableDefinition] =
    userTableDefinitions(tables.map(mapAWSGlueTable), userDefinedTable)

  private def mapSchemaCrawlerTable(table: SchemaCrawlerTable): HadoopTable = {
    HadoopTable(
      `type` = "Hadoop",
      sourceName = table.getName,
      destinationName = cleanseName(table.getName),
      checkColumn = None,
      comment = Option(table.getRemarks),
      primaryKeys = table.getColumns.asScala.filter(c => c.isPartOfPrimaryKey).map(_.getName),
      metadata = None,
      numberOfMappers = Some(1),
      splitByColumn = None,
      numberOfPartitions = Some(1),
      columns = table.getColumns.asScala.map(mapSchemaCrawlerColumnDefinition)
    )
  }

  private def mapAWSGlueTable(table: AWSTable): HadoopTable = {
    HadoopTable(
      `type` = "Hadoop",
      sourceName = table.getName,
      destinationName = cleanseName(table.getName),
      checkColumn = None,
      comment = Option(table.getDescription),
      primaryKeys = Seq(),
      metadata = None,
      numberOfMappers = Some(1),
      splitByColumn = None,
      numberOfPartitions = Some(1),
      columns = table.getStorageDescriptor.getColumns.asScala.map(mapAWSGlueColumnDefinition)
    )
  }

  private def mapSchemaCrawlerColumnDefinition(column: SchemaCrawlerColumn): ColumnDefinition =
    ColumnDefinition(
      sourceName = column.getName,
      destinationName = cleanseName(column.getName),
      dataType = column.getColumnDataType.getName,
      comment = Option(column.getRemarks),
      precision = Option(column.getSize),
      scale = Option(column.getDecimalDigits)
    )

  private def mapAWSGlueColumnDefinition(column: AWSColumn): ColumnDefinition =
    ColumnDefinition(
      sourceName = column.getName,
      destinationName = cleanseName(column.getName),
      dataType = column.getType,
      comment = Option(column.getComment)
    )

  private def userTableDefinitions(
      tables: Seq[HadoopTable],
      userDefinedTablesOpt: Option[Seq[UserDefinedTable]]): Seq[HadoopTable] = {
    userDefinedTablesOpt match {
      case Some(userDefinedTables) =>
        tables.map { table =>
          val userTables = userDefinedTables.map(ut => ut.asInstanceOf[HadoopUserDefinedTable])
          userTables.find(ut => ut.name.equalsIgnoreCase(table.sourceName)) match {
            case Some(userTable) =>
              val enhancedMetadata = table.metadata match {
                case Some(metadata) =>
                  userTable.metadata match {
                    case Some(userMetadata) => Some(metadata ++ userMetadata)
                    case None => None
                  }
                case None => userTable.metadata
              }

              val primaryKeys = userTable.primaryKeys match {
                case Some(pks) => pks
                case None => table.primaryKeys
              }

              table.copy(
                primaryKeys = primaryKeys,
                checkColumn = userTable.checkColumn,
                numberOfMappers = userTable.numberOfMappers,
                splitByColumn = userTable.splitByColumn,
                numberOfPartitions = userTable.numberOfPartitions,
                metadata = enhancedMetadata
              )
            case None => table
          }
        }
      case None => tables
    }

  }
}
