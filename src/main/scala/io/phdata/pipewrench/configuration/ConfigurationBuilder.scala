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
import org.slf4j.LoggerFactory
import schemacrawler.schema.{Column => SchemaCrawlerColumn}
import schemacrawler.schema.{Table => SchemaCrawlerTable}

import collection.JavaConverters._

object ConfigurationBuilder extends YamlSupport {

  private lazy val logger = LoggerFactory.getLogger(ConfigurationBuilder.getClass)

  def build(configurationFile: String, password: String): Configuration = {
    build(readConfigurationFile(configurationFile), password)
  }

  def build(configuration: Configuration, password: String): Configuration = {
    val catalog = SchemaCrawlerImpl.getCatalog(configuration.jdbc, password)

    val tables =
      catalog.getSchemas.asScala.find(s => s.getFullName.equals(configuration.jdbc.schema)) match {
        case Some(schema) =>
          catalog
            .getTables(schema)
            .asScala
            .map { parsedTable =>
              configuration.jdbc.tables match {
                case Some(userDefinedTables) =>
                  userDefinedTables.find(t => t.name.equals(parsedTable.getName)) match {
                    case Some(userDefinedTable) =>
                      enhanceTableDefinition(mapTableDefinition(parsedTable), userDefinedTable)
                    case None => mapTableDefinition(parsedTable)
                  }
                case None =>
                  mapTableDefinition(parsedTable)
              }
            }
            .toSeq
        case None =>
          throw new RuntimeException(
            s"Schema: ${configuration.jdbc.schema}, does not exist in source system")
      }

    val enhancedConfiguration = configuration.copy(
      jdbc =
        configuration.jdbc.copy(driverClass = Some(catalog.getJdbcDriverInfo.getDriverClassName)),
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
    SchemaCrawlerImpl.getErdOutput(configuration.jdbc, databasePassword, path)
    SchemaCrawlerImpl.getHtmlOutput(configuration.jdbc, databasePassword, path)
  }

  private def checkConfiguration(configuration: Configuration): Unit = {
    for (table <- configuration.tables.get) {
      if (configuration.pipeline.equalsIgnoreCase("INCREMENTAL-WITH-KUDU")) {
        checkPrimaryKeys(table)
        checkCheckColumn(table)
      } else if (configuration.pipeline.equalsIgnoreCase("KUDU-TABLE-DLL")) {
        checkPrimaryKeys(table)
      }
    }
  }

  private def checkPrimaryKeys(table: TableDefinition): Unit = {
    if (table.primaryKeys.isEmpty) {
      logger.warn(
        s"No primary keys are defined for table: ${table.sourceName}, kudu table creation will fail for this table unless the primary keys are added manually")
    }
  }

  private def checkCheckColumn(table: TableDefinition): Unit = {
    if (table.checkColumn.isEmpty) {
      logger.warn(
        s"No check column is defined for table: ${table.sourceName}, sqoop incremental import will fail for this table unless the checkColumn is added manually")
    }
  }

  private def mapTableDefinition(table: SchemaCrawlerTable): TableDefinition = {
    TableDefinition(
      sourceName = table.getName,
      destinationName = TemplateFunction.cleanse(table.getName),
      comment = Option(table.getRemarks),
      primaryKeys = table.getColumns.asScala.filter(c => c.isPartOfPrimaryKey).map(_.getName),
      columns = table.getColumns.asScala.map(mapColumnDefinition)
    )
  }

  private def enhanceTableDefinition(
      table: TableDefinition,
      userDefined: Table): TableDefinition = {
    checkNumberOfMappers(userDefined)

    table.copy(
      checkColumn = userDefined.checkColumn,
      numberOfMappers = userDefined.numberOfMappers,
      splitByColumn = userDefined.splitByColumn,
      numberOfPartitions = userDefined.numberOfPartitions,
      metadata = userDefined.metadata
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
