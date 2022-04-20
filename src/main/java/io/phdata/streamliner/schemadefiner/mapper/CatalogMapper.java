// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package io.phdata.streamliner.schemadefiner.mapper;

import io.phdata.streamliner.schemadefiner.model.*;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import schemacrawler.crawl.StreamlinerCatalog;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;

public class CatalogMapper {

  private final Configuration ingestConfig;
  private final StreamlinerCatalog catalog;
  private final String jdbcPassword;

  public CatalogMapper(
      final Configuration ingestConfig,
      final StreamlinerCatalog catalog,
      final String jdbcPassword) {
    this.ingestConfig = ingestConfig;
    this.catalog = catalog;
    this.jdbcPassword = jdbcPassword;
  }

  public Configuration mapJdbcCatalogToConfig() {
    Jdbc jdbc = (Jdbc) ingestConfig.getSource();
    // here filtering of schema will be valid if catalog will come from schema crawler library.
    List<Schema> schema =
        catalog.getSchemas().stream().filter(matchSchemaName(jdbc)).collect(Collectors.toList());
    if (schema.isEmpty()) {
      throw new IllegalStateException(String.format("No result found for %s", jdbc.getSchema()));
    } else if (schema.size() > 1) {
      throw new IllegalStateException(
          String.format("Found more than one result for %s: %s", jdbc.getSchema(), schema));
    }
    List<Table> tableList = (List<Table>) catalog.getTables(schema.get(0));
    if (tableList == null || tableList.isEmpty()) {
      throw new RuntimeException(
          String.format(
              "%s schema does not  have %s in source system",
              jdbc.getSchema(), jdbc.getTableTypes().toString()));
    }
    if (jdbc.getIgnoreTables() != null && !jdbc.getIgnoreTables().isEmpty()) {
      tableList =
          tableList.stream()
              .filter(table -> !jdbc.getIgnoreTables().contains(table.getName()))
              .collect(Collectors.toList());
    }
    if (tableList.isEmpty()) {
      throw new RuntimeException("No tables found or all tables ignored.");
    }
    List<TableDefinition> tables = null;
    if (ingestConfig.getDestination() instanceof Snowflake) {
      tables =
          new SnowflakeMapper(jdbc, (Snowflake) ingestConfig.getDestination(), jdbcPassword)
              .mapSchemaCrawlerTables(tableList);
    } else if (ingestConfig.getDestination() instanceof Hadoop) {
      tables = HadoopMapper.mapSchemaCrawlerTables(tableList, jdbc.getUserDefinedTable());
    } else {
      throw new RuntimeException(
          String.format(
              "Unknown Destination provided: %s", ingestConfig.getDestination().getType()));
    }
    jdbc.setDriverClass(catalog.getDriverClassName());
    Configuration newConfig =
        new Configuration(
            ingestConfig.getName(),
            ingestConfig.getEnvironment(),
            ingestConfig.getPipeline(),
            ingestConfig.getSource(),
            ingestConfig.getDestination(),
            ingestConfig.getGenericProperties(),
            tables);
    return newConfig;
  }

  private Predicate<Schema> matchSchemaName(Jdbc jdbc) {
    return catalogSchema ->
        jdbc.getSchema().equals(catalogSchema.getName())
            || jdbc.getSchema().equals(catalogSchema.getFullName());
  }
}
