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
package io.phdata.streamliner.schemadefiner;

import io.phdata.streamliner.schemadefiner.model.Jdbc;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import java.sql.Connection;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.crawl.StreamlinerCatalog;
import schemacrawler.crawl.StreamlinerSchemaCrawler;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;

public class JdbcCrawler implements SchemaDefiner {

  private static final Logger log = LoggerFactory.getLogger(JdbcCrawler.class);

  private String password;
  private final Supplier<Connection> connectionSupplier;
  private final SchemaCrawlerOptions schemaCrawlerOptions;
  private List<String> tableTypes;
  private Supplier<Connection> conSupplier =
      new Supplier<Connection>() {
        @Override
        public Connection get() {
          return StreamlinerUtil.getConnection(jdbc.getUrl(), jdbc.getUsername(), password);
        }
      };
  private Jdbc jdbc;

  public JdbcCrawler(Jdbc jdbc, String password) {
    this.jdbc = jdbc;
    this.password = password;
    this.tableTypes = StreamlinerUtil.mapTableTypes(jdbc.getTableTypes());
    this.connectionSupplier = conSupplier;
    this.schemaCrawlerOptions = StreamlinerUtil.getOptions(jdbc);
  }

  @Override
  public StreamlinerCatalog retrieveSchema() {
    try {
      return StreamlinerSchemaCrawler.getCatalog(
          jdbc, connectionSupplier, schemaCrawlerOptions, tableTypes);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error in JdbcCrawler: %s", e), e);
    }
  }
}
