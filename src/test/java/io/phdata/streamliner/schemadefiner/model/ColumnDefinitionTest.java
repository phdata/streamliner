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
package io.phdata.streamliner.schemadefiner.model;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class ColumnDefinitionTest {

  @Test
  public void testLongColumnType() {
    ColumnDefinition c = new ColumnDefinition("c1", "c1", "VARCHAR", null, 17_777_216, null, false);
    Map<String, Map<String, String>> typeMap = new HashMap<>();
    Map<String, String> subMap = new HashMap<>();
    subMap.put("snowflake", "varchar");
    typeMap.put("varchar", subMap);
    String result = c.mapDataTypeSnowflake(typeMap);
    Assert.assertEquals("VARCHAR(16777216)", result);
  }
}
