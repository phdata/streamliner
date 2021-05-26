package io.phdata.streamliner.configuration.parser

import io.phdata.streamliner.configuration._
import io.phdata.streamliner.configuration.mapper.HadoopTableMapper
import io.phdata.streamliner.configuration.mapper.SnowflakeTableMapper
import io.phdata.streamliner.schemacrawler.SchemaCrawlerImpl
import org.apache.log4j.Logger

import collection.JavaConverters._

object JDBCParser {

  private lazy val logger = Logger.getLogger(ConfigurationBuilder.getClass)

  def parse(configuration: Configuration, password: String): Configuration = {
    val jdbc = configuration.source.asInstanceOf[Jdbc]
    val catalog = SchemaCrawlerImpl.getCatalog(jdbc, password)

    val tables =
      catalog.getSchemas.asScala.find(s => s.getFullName.contains(jdbc.schema)) match {
        case Some(schema) =>
          val schemaCrawlerTables = catalog.getTables(schema).asScala.toList
          configuration.destination match {
            case hadoop: Hadoop =>
              HadoopTableMapper.mapSchemaCrawlerTables(schemaCrawlerTables, jdbc.userDefinedTable)
            case snowflake: Snowflake =>
              SnowflakeTableMapper
                .mapSchemaCrawlerTables(schemaCrawlerTables, jdbc.userDefinedTable)
          }
        case None =>
          throw new RuntimeException(s"Schema: ${jdbc.schema}, does not exist in source system")
      }

    configuration.copy(
      source = jdbc.copy(driverClass = Some(catalog.getDriverClassName)),
      tables = Some(tables)
    )
  }

}
