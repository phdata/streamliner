package io.phdata.streamliner.configuration.parser

import com.amazonaws.services.glue.AWSGlueClient
import com.amazonaws.services.glue.model.GetTablesRequest
import com.amazonaws.services.glue.model.{Table => AWSTable}
import io.phdata.streamliner.configuration._
import io.phdata.streamliner.configuration.mapper.HadoopTableMapper
import io.phdata.streamliner.configuration.mapper.SnowflakeTableMapper

import collection.JavaConverters._

object GlueCatalogParser {

  def parse(configuration: Configuration): Configuration = {
    val glueCatalog = configuration.source.asInstanceOf[GlueCatalog]
    val glueTables = getTables(glueCatalog.region, glueCatalog.database)
    val tables = configuration.destination match {
      case hadoop: Hadoop =>
        HadoopTableMapper.mapAWSGlueTables(glueTables, glueCatalog.userDefinedTable)
      case snowflake: Snowflake =>
        SnowflakeTableMapper.mapAWSGlueTables(glueTables, glueCatalog.userDefinedTable)
    }
    configuration.copy(tables = Some(tables))
  }

  private def getTables(region: String, database: String): List[AWSTable] = {
    val client = AWSGlueClient.builder().withRegion(region).build()
    val tables = client.getTables(new GetTablesRequest().withDatabaseName(database))
    tables.getTableList.asScala.toList
  }

}
