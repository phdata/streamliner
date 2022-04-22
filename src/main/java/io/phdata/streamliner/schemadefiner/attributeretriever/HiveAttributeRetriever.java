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
package io.phdata.streamliner.schemadefiner.attributeretriever;

import io.phdata.streamliner.schemadefiner.model.FileFormat;
import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveAttributeRetriever {
  private static final Logger log = LoggerFactory.getLogger(HiveAttributeRetriever.class);
  private final Connection con;

  private final String HIVE_USE_DATABASE = "USE %s;";
  private final String HIVE_DESCRIBE_TABLE = "DESCRIBE FORMATTED %s;";

  private static final Map<String, String> hiveSnowflakeFileTypeMapping = new HashMap<>();

  static {
    hiveSnowflakeFileTypeMapping.put("org.apache.hadoop.mapred.TextInputFormat", "CSV");
    hiveSnowflakeFileTypeMapping.put(
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", "AVRO");
    hiveSnowflakeFileTypeMapping.put(
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "PARQUET");
    hiveSnowflakeFileTypeMapping.put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "ORC");
  }

  public HiveAttributeRetriever(Connection con) {
    this.con = con;
  }

  public List<Map<String, Object>> retrieve(String database, String tableName) {
    log.info("Retrieving additional attributes information for table {}.{}", database, tableName);
    List<Map<String, Object>> result = Collections.synchronizedList(new ArrayList<>());

    try (Statement statement = con.createStatement()) {
      String useDbQuery = String.format(HIVE_USE_DATABASE, database);
      log.info("Executing query: {}", useDbQuery);
      statement.execute(useDbQuery);

      String describeTableQuery = String.format(HIVE_DESCRIBE_TABLE, tableName);
      log.info("Executing query: {}", describeTableQuery);
      ResultSet rs = statement.executeQuery(describeTableQuery);
      ResultSetMetaData rsmd = rs.getMetaData();

      while (rs.next()) {
        int count = rsmd.getColumnCount();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i < count + 1; i++) {
          row.put(rsmd.getColumnLabel(i), rs.getObject(i));
        }
        result.add(row);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          String.format("Error while processing database: {}, table: {}", database, tableName), e);
    }
    return result;
  }

  public FileFormat calculateFileFormat(String database, String tableName) {
    List<Map<String, Object>> hiveAttributes = retrieve(database, tableName);
    String fileType = "";
    String location = "";
    for (Map<String, Object> attribute : hiveAttributes) {
      if (((String) attribute.get("name")).contains("Location:")) {
        location = (String) attribute.get("type");
      }
      if (((String) attribute.get("name")).contains("InputFormat:")) {
        fileType = (String) attribute.get("type");
      }
    }
    return new FileFormat(location, hiveSnowflakeFileTypeMapping.getOrDefault(fileType, fileType));
  }
}
