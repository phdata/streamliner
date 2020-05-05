package io.phdata.streamliner.configuration

import com.amazonaws.services.glue.model.{Column, GetTablesRequest, Table => AWSTable, Column => AWSColumn}
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClientBuilder}
import io.phdata.streamliner.util.TemplateFunction

import collection.JavaConverters._

object GlueCatalogParser {

  def parse(configuration: Configuration): Configuration = {
    val glueCatalog = configuration.source.asInstanceOf[GlueCatalog]
    val client = new AWSGlueClientBuilder().withRegion(glueCatalog.region).build()
    val tables = getTables(client, glueCatalog.database).map(mapTable)
    configuration.copy(tables = Some(tables))
  }

  private def getTables(client: AWSGlue, database: String): List[AWSTable] = {
    val tables = client.getTables(new GetTablesRequest().withDatabaseName(database))
    tables.getTableList.asScala.toList
  }

  private def mapTable(table: AWSTable): TableDefinition =
    TableDefinition(
      sourceName = table.getName,
      destinationName = TemplateFunction.cleanse(table.getName),
      comment = Option(table.getDescription),
      primaryKeys = Seq(),
      storage = Some(
        StorageDefinition(
          location = table.getStorageDescriptor.getLocation,
          fileType = table.getStorageDescriptor.getOutputFormat
        )
      ),
      columns = table.getStorageDescriptor.getColumns.asScala.map(mapColumn)
    )

  private def mapColumn(column: AWSColumn): ColumnDefinition =
    ColumnDefinition(
      sourceName = column.getName,
      destinationName = TemplateFunction.cleanse(column.getName),
      dataType = column.getType,
      comment = Option(column.getComment)
    )

}
