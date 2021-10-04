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

import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import java.util.*;
import org.junit.Assert;
import org.junit.Test;

public class TableDiffTest {
  private final String baseConfigDiffPath = "src/test/resources/validSchemaTestConfigDiff";
  private final String typeMappingFilePath = "src/test/resources/type-mapping.yml";

  @Test
  public void testAllChangesAreCompatible_allValidSchemaChanges() {

    ConfigurationDiff configDiff =
        StreamlinerUtil.readConfigDiffFromPath(
            String.format("%s/%s", baseConfigDiffPath, "streamliner-diff.yml"));
    Map<String, Map<String, String>> typeMapping =
        StreamlinerUtil.readTypeMappingFile(typeMappingFilePath);
    TableDiff tableDiff = configDiff.getTableDiffs().get(0);
    boolean flag =
        tableDiff.allChangesAreCompatible(
            JavaHelper.convertJavaMapToScalaMap(typeMapping),
            new HashSet<>(Arrays.asList(SchemaChanges.values())));
    Assert.assertTrue(flag);
  }

  @Test
  public void testAllChangesAreCompatible_exclude_columnAdd() {

    ConfigurationDiff configDiff =
        StreamlinerUtil.readConfigDiffFromPath(
            String.format("%s/%s", baseConfigDiffPath, "streamliner-diff.yml"));
    Map<String, Map<String, String>> typeMapping =
        StreamlinerUtil.readTypeMappingFile(typeMappingFilePath);
    TableDiff tableDiff = configDiff.getTableDiffs().get(0);
    // excluded COLUMN_ADD
    Set<SchemaChanges> validSchemaChanges =
        new HashSet<>(
            Arrays.asList(
                new SchemaChanges[] {
                  SchemaChanges.UPDATE_COLUMN_COMMENT,
                  SchemaChanges.TABLE_ADD,
                  SchemaChanges.EXTEND_COLUMN_LENGTH,
                  SchemaChanges.UPDATE_COLUMN_NULLABILITY
                }));

    boolean flag =
        tableDiff.allChangesAreCompatible(
            JavaHelper.convertJavaMapToScalaMap(typeMapping), validSchemaChanges);
    Assert.assertFalse(flag);
  }
}
