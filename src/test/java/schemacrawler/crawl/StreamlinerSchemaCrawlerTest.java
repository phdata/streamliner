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

import static schemacrawler.crawl.StreamlinerSchemaCrawler.*;

import java.sql.SQLException;
import java.util.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaReference;

public class StreamlinerSchemaCrawlerTest {

  private Map<String, List<Map<String, Object>>> resultSet;
  private static final String SCHEMA = "s1";

  @Before
  public void setup() {
    resultSet = new HashMap<>();
  }

  @Test
  public void testEmpty() throws Exception {
    StreamlinerCatalog catalog =
        StreamlinerSchemaCrawler.getOracleCatalog(
            new TestQueryHandler(), SCHEMA, Collections.singletonList("table"), null);
    Assert.assertEquals(SCHEMA, catalog.getSchemas().iterator().next().getName());
    Assert.assertEquals(
        Collections.emptyList(), catalog.getTables(new SchemaReference(null, SCHEMA)));
  }

  @Test
  public void testFullCatalog() throws Exception {
    Map<String, Object> table1 = new HashMap<>();
    table1.put("TABLE_NAME", "t1");
    table1.put("REMARKS", "Something");
    table1.put("TABLE_TYPE", "TABLE");
    Map<String, Object> table2 = new HashMap<>();
    table2.put("TABLE_NAME", "v1");
    table2.put("TABLE_TYPE", "VIEW");
    Map<String, Object> column1 = new HashMap<>();
    column1.put("TABLE_NAME", "t1");
    column1.put("COLUMN_NAME", "c1");
    column1.put("TYPE_NAME", "varchar2");
    column1.put("ORDINAL_POSITION", 1);
    Map<String, Object> column2 = new HashMap<>();
    column2.put("TABLE_NAME", "t1");
    column2.put("COLUMN_NAME", "c2");
    column2.put("TYPE_NAME", "number");
    column2.put("ORDINAL_POSITION", 2);
    Map<String, Object> constraint = new HashMap<>();
    constraint.put("TABLE_NAME", "t1");
    constraint.put("COLUMN_NAME", "c1");
    constraint.put("CONSTRAINT_TYPE", "P");
    Map<String, Object> index = new HashMap<>();
    index.put("TABLE_NAME", "t1");
    index.put("COLUMN_NAME", "c2");
    resultSet.put(ORACLE_TABLES_TEMPLATE, Arrays.asList(table1, table2));
    resultSet.put(ORACLE_COLUMNS_TEMPLATE, Arrays.asList(column1, column2));
    resultSet.put(ORACLE_CONSTRAINTS_TEMPLATE, Arrays.asList(constraint));
    resultSet.put(ORACLE_UNIQUE_INDEXES_TEMPLATE, Arrays.asList(index));
    StreamlinerCatalog catalog =
        StreamlinerSchemaCrawler.getOracleCatalog(
            new TestQueryHandler(), SCHEMA, Arrays.asList("table", "view"), null);
    List<Table> result = new ArrayList<>(catalog.getTables(new SchemaReference(SCHEMA, SCHEMA)));
    Assert.assertEquals("t1", result.get(0).getName());
    Assert.assertEquals("v1", result.get(1).getName());
    Assert.assertEquals("c1", result.get(0).getColumns().get(0).getName());
    Assert.assertEquals("c2", result.get(0).getColumns().get(1).getName());
    Assert.assertTrue(result.get(0).getColumns().get(0).isPartOfPrimaryKey());
    Assert.assertTrue(result.get(0).getColumns().get(1).isPartOfUniqueIndex());
  }

  private class TestQueryHandler extends StreamlinerSchemaCrawler.QueryHandler {
    public TestQueryHandler() {
      super(
          () -> {
            throw new IllegalStateException();
          });
    }

    protected List<Map<String, Object>> queryToList(String query, String schemaName)
        throws SQLException, InterruptedException {
      if (resultSet.containsKey(query)) {
        return resultSet.get(query);
      }
      return Collections.emptyList();
    }
  }
}
