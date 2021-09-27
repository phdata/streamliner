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
package schemacrawler.crawl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;

public class StreamlinerCatalog {
  private final List<Schema> schemas;
  private final Map<Schema, List<Table>> tables;
  private final String driverClassName;

  public StreamlinerCatalog(
      String driverClassName, List<Schema> schemas, Map<Schema, List<Table>> tables) {
    this.driverClassName = driverClassName;
    this.schemas = schemas;
    this.tables = tables;
  }

  public Collection<Schema> getSchemas() {
    return schemas;
  }

  public Collection<Table> getTables(Schema schema) {
    if (tables.containsKey(schema)) {
      return tables.get(schema);
    }
    return Collections.emptyList();
  }

  public String getDriverClassName() {
    return driverClassName;
  }
}
