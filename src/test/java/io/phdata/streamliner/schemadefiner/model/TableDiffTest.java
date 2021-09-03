package io.phdata.streamliner.schemadefiner.model;

import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

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
