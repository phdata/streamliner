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

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import io.phdata.streamliner.schemadefiner.model.FileFormat;
import io.phdata.streamliner.schemadefiner.model.GlueCatalog;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import schemacrawler.crawl.SchemaDefinerHelper;
import schemacrawler.crawl.StreamlinerCatalog;

public class GlueCrawler implements SchemaDefiner {

  private String database;
  private String region;
  private static Map<schemacrawler.schema.Table, FileFormat> tableFileFormatMap = new HashMap<>();

  public GlueCrawler(GlueCatalog glueCatalog) {
    this.region = glueCatalog.getRegion();
    this.database = glueCatalog.getDatabase();
  }

  @Override
  public StreamlinerCatalog retrieveSchema() {
    List<Table> tableList = getTables(region, database);
    return SchemaDefinerHelper.mapAWSTableToCatalog(tableList, database, tableFileFormatMap);
  }

  private List<Table> getTables(String region, String database) {
    AWSGlue client = AWSGlueClient.builder().withRegion(region).build();
    GetTablesResult tables = client.getTables(new GetTablesRequest().withDatabaseName(database));
    return tables.getTableList();
  }

  public static Map<schemacrawler.schema.Table, FileFormat> getTableFileFormatMap() {
    return tableFileFormatMap;
  }
}
