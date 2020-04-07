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

package io.phdata.pipewrench.configuration

import io.phdata.pipewrench.schemacrawler.SchemaCrawlerImpl
import io.phdata.pipewrench.util.TemplateFunction
import org.apache.log4j.Logger
import schemacrawler.schema.{Column => SchemaCrawlerColumn}
import schemacrawler.schema.{Table => SchemaCrawlerTable}

import collection.JavaConverters._

object ConfigurationBuilder extends YamlSupport {

  private lazy val logger = Logger.getLogger(ConfigurationBuilder.getClass)

  def build(configurationFile: String, password: String): Configuration = {
    build(readConfigurationFile(configurationFile), password)
  }

  def build(configuration: Configuration, password: String): Configuration = {
    val jdbc = configuration.source.asInstanceOf[Jdbc]
    val catalog = SchemaCrawlerImpl.getCatalog(jdbc, password)

    val tables =
      catalog.getSchemas.asScala.find(s => s.getFullName.equals(jdbc.schema)) match {
        case Some(schema) =>
          catalog
            .getTables(schema)
            .asScala
            .map { parsedTable =>
              jdbc.tables match {
                case Some(userDefinedTables) =>
                  userDefinedTables.find(t => t.name.equals(parsedTable.getName)) match {
                    case Some(userDefinedTable) =>
                      enhanceTableDefinition(
                        mapTableDefinition(parsedTable, jdbc.metadata),
                        userDefinedTable)
                    case None => mapTableDefinition(parsedTable, jdbc.metadata)
                  }
                case None =>
                  mapTableDefinition(parsedTable, jdbc.metadata)
              }
            }
            .toSeq
        case None =>
          throw new RuntimeException(s"Schema: ${jdbc.schema}, does not exist in source system")
      }

    val enhancedConfiguration = configuration.copy(
      source = jdbc.copy(driverClass = Some(catalog.getJdbcDriverInfo.getDriverClassName)),
      tables = Some(tables)
    )

    checkConfiguration(enhancedConfiguration)
    enhancedConfiguration
  }

  def write(configuration: Configuration, outputDirectory: Option[String] = None): Unit = {
    val dir = outputDirectory.fold(
      s"output/${configuration.name}/${configuration.environment}/conf")(output => s"$output/conf")
    writeConfiguration(configuration, dir)
  }

  def writeDocs(
      configuration: Configuration,
      databasePassword: String,
      outputDirectory: Option[String] = None): Unit = {
    val path =
      outputDirectory.fold(s"output/${configuration.name}/${configuration.environment}/docs")(p =>
        s"$p/docs")
    val jdbc = configuration.source.asInstanceOf[Jdbc]
    SchemaCrawlerImpl.getErdOutput(jdbc, databasePassword, path)
    SchemaCrawlerImpl.getHtmlOutput(jdbc, databasePassword, path)
  }

  private def checkConfiguration(configuration: Configuration): Unit = {
    for (table <- configuration.tables.get) {
      if (configuration.pipeline.equalsIgnoreCase("INCREMENTAL-WITH-KUDU")) {
        checkPrimaryKeys(table)
        checkCheckColumn(table)
      } else if (configuration.pipeline.equalsIgnoreCase("KUDU-TABLE-DLL") || configuration.pipeline
          .equalsIgnoreCase("SNOWFLAKE-DMS-CDC")) {
        checkPrimaryKeys(table)
      }
    }
  }

  private def checkPrimaryKeys(table: TableDefinition): Unit = {
    if (table.primaryKeys.isEmpty) {
      logger.warn(s"No primary keys are defined for table: ${table.sourceName}")
    }
  }

  private def checkCheckColumn(table: TableDefinition): Unit = {
    if (table.checkColumn.isEmpty) {
      logger.warn(
        s"No check column is defined for table: ${table.sourceName}, sqoop incremental import will fail for this table unless the checkColumn is added manually")
    }
  }

  private def mapTableDefinition(
      table: SchemaCrawlerTable,
      metadataOpt: Option[Map[String, String]] = None): TableDefinition = {
    TableDefinition(
      sourceName = table.getName,
      destinationName = TemplateFunction.cleanse(table.getName),
      comment = Option(table.getRemarks),
      primaryKeys = table.getColumns.asScala.filter(c => c.isPartOfPrimaryKey).map(_.getName),
      columns = table.getColumns.asScala.map(mapColumnDefinition),
      metadata = metadataOpt
    )
  }

  private def enhanceTableDefinition(
      table: TableDefinition,
      userDefined: Table): TableDefinition = {
    checkNumberOfMappers(userDefined)

    val enhancedMetadata = table.metadata match {
      case Some(metadata) =>
        userDefined.metadata match {
          case Some(userMetadata) => Some(metadata ++ userMetadata)
          case None => None
        }
      case None => userDefined.metadata
    }

    table.copy(
      checkColumn = userDefined.checkColumn,
      numberOfMappers = userDefined.numberOfMappers,
      splitByColumn = userDefined.splitByColumn,
      numberOfPartitions = userDefined.numberOfPartitions,
      metadata = enhancedMetadata
    )
  }

  private def mapColumnDefinition(column: SchemaCrawlerColumn): ColumnDefinition =
    ColumnDefinition(
      sourceName = column.getName,
      destinationName = TemplateFunction.cleanse(column.getName),
      dataType = column.getColumnDataType.getName,
      comment = Option(column.getRemarks),
      precision = Option(column.getSize),
      scale = Option(column.getDecimalDigits)
    )

  private def checkNumberOfMappers(table: Table) = {
    table.numberOfMappers match {
      case Some(numberOfMappers) =>
        if (numberOfMappers > 1) {
          if (table.splitByColumn.isEmpty) {
            throw new RuntimeException(
              s"Table: $table, has number of mappers greater than 1 with no splitByColumn defined Sqoop import will fail for this table")
          }
        }
      case None => // no-op
    }
  }

}
