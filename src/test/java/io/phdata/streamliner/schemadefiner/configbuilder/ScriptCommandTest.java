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
package io.phdata.streamliner.schemadefiner.configbuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.ConfigurationDiff;
import io.phdata.streamliner.schemadefiner.model.Constants;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ScriptCommandTest {
  private String templateDirectory = "src/test/resources/templates/snowflake";
  private String typeMapping = "src/main/resources/type-mapping.yml";
  private String outputDir = "src/test/resources/results/scriptCommand";
  String config = "src/test/resources/conf/ingest-configuration.yml";

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testScriptCommandForSchemaEvolution_Alter_Table_Add_column() {
    String stateDirectory = "src/test/resources/configDiff/addColumn/state-directory";
    String previousDirectory = "src/test/resources/configDiff/addColumn/previous-state-directory";
    Configuration prevConfig =
        StreamlinerUtil.createConfig(
            previousDirectory, StreamlinerUtil.readYamlFile(config), "--previous-state-directory");
    Configuration currConfig =
        StreamlinerUtil.createConfig(
            stateDirectory, StreamlinerUtil.readYamlFile(config), "--state-directory");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(
        diff, String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));

    outputDir =
        String.format(
            "%s/%s", outputDir, "testScriptCommandForSchemaEvolution_Alter_Table_Add_column");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config, stateDirectory, previousDirectory, typeMapping, templateDirectory, outputDir);
    File f = new File(String.format("%s/Employee", outputDir));
    assertTrue(f.exists());
    assertTrue(f.isDirectory());
    assertEquals(13, f.list().length);
  }

  @Test
  public void testScriptCommandForSchemaEvolution_Alter_Table_Modify_column() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("require investigation");
    String stateDirectory = "src/test/resources/configDiff/changeColumn/state-directory";
    String previousDirectory =
        "src/test/resources/configDiff/changeColumn/previous-state-directory";
    Configuration prevConfig =
        StreamlinerUtil.createConfig(
            previousDirectory, StreamlinerUtil.readYamlFile(config), "--previous-state-directory");
    Configuration currConfig =
        StreamlinerUtil.createConfig(
            stateDirectory, StreamlinerUtil.readYamlFile(config), "--state-directory");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(
        diff, String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));

    outputDir =
        String.format(
            "%s/%s", outputDir, "testScriptCommandForSchemaEvolution_Alter_Table_Modify_column");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config, stateDirectory, previousDirectory, typeMapping, templateDirectory, outputDir);
  }

  @Test
  public void testScriptCommandForSchemaEvolution_Create_Table() {
    String stateDirectory = "src/test/resources/configDiff/addTable/state-directory";
    String previousDirectory = "src/test/resources/configDiff/addTable/previous-state-directory";
    Configuration prevConfig =
        StreamlinerUtil.createConfig(
            previousDirectory, StreamlinerUtil.readYamlFile(config), "--previous-state-directory");
    Configuration currConfig =
        StreamlinerUtil.createConfig(
            stateDirectory, StreamlinerUtil.readYamlFile(config), "--state-directory");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(
        diff, String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));

    outputDir =
        String.format("%s/%s", outputDir, "testScriptCommandForSchemaEvolution_Create_Table");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config, stateDirectory, previousDirectory, typeMapping, templateDirectory, outputDir);
    File f = new File(String.format("%s/Department", outputDir));
    assertTrue(f.exists());
    assertTrue(f.isDirectory());
    assertEquals(13, f.list().length);
  }

  @Test
  public void testScriptCommandToGenerateScripts() {
    String stateDirectory = "src/test/resources/configDiff/addAllColumn/state-directory";
    String previousDirectory =
        "src/test/resources/configDiff/addAllColumn/previous-state-directory";
    outputDir = String.format("%s/%s", outputDir, "testScriptCommandToGenerateScripts");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config, stateDirectory, previousDirectory, typeMapping, templateDirectory, outputDir);

    File f = new File(String.format("%s/Employee", outputDir));
    assertTrue(f.exists());
    assertTrue(f.isDirectory());
    assertEquals(13, f.list().length);
  }

  @Test
  public void testScriptCommand_mandatory_config() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage(
        "--state-directory has no table config and streamliner-diff.yml not found.");
    String stateDirectory = "src/test/resources/configDiff/mandatoryTableConfig/state-directory";
    String previousDirectory =
        "src/test/resources/configDiff/changeColumn/previous-state-directory";
    outputDir = String.format("%s/%s", outputDir, "testScriptCommand_mandatory_config");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config, stateDirectory, previousDirectory, typeMapping, templateDirectory, outputDir);
  }

  @Test
  public void testScriptCommandForSchemaEvolution_column_size_increased() {
    String stateDirectory = "src/test/resources/configDiff/columnSizeChanged/state-directory";
    String previousDirectory =
        "src/test/resources/configDiff/columnSizeChanged/previous-state-directory";
    Configuration prevConfig =
        StreamlinerUtil.createConfig(
            previousDirectory, StreamlinerUtil.readYamlFile(config), "--previous-state-directory");
    Configuration currConfig =
        StreamlinerUtil.createConfig(
            stateDirectory, StreamlinerUtil.readYamlFile(config), "--state-directory");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(
        diff, String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));

    outputDir =
        String.format(
            "%s/%s", outputDir, "testScriptCommandForSchemaEvolution_column_size_increased");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config, stateDirectory, previousDirectory, typeMapping, templateDirectory, outputDir);

    File f = new File(String.format("%s/Employee", outputDir));
    assertTrue(f.exists());
    assertTrue(f.isDirectory());
    assertEquals(13, f.list().length);
  }

  @Test
  public void testScriptCommandForSchemaEvolution_column_comment_nullable_changed() {
    String stateDirectory =
        "src/test/resources/configDiff/columnCommentAndNullable/state-directory";
    String previousDirectory =
        "src/test/resources/configDiff/columnCommentAndNullable/previous-state-directory";
    Configuration prevConfig =
        StreamlinerUtil.createConfig(
            previousDirectory, StreamlinerUtil.readYamlFile(config), "--previous-state-directory");
    Configuration currConfig =
        StreamlinerUtil.createConfig(
            stateDirectory, StreamlinerUtil.readYamlFile(config), "--state-directory");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(
        diff, String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));

    outputDir =
        String.format(
            "%s/%s",
            outputDir, "testScriptCommandForSchemaEvolution_column_comment_nullable_changed");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config, stateDirectory, previousDirectory, typeMapping, templateDirectory, outputDir);

    File f = new File(String.format("%s/Employee", outputDir));
    assertTrue(f.exists());
    assertTrue(f.isDirectory());
    assertEquals(13, f.list().length);
  }

  @Test
  public void testScriptCommandForSchemaEvolution_no_schema_changes() {
    String stateDirectory = "src/test/resources/configDiff/noSchemaChanges/state-directory";
    String previousDirectory =
        "src/test/resources/configDiff/noSchemaChanges/previous-state-directory";
    Configuration prevConfig =
        StreamlinerUtil.createConfig(
            previousDirectory, StreamlinerUtil.readYamlFile(config), "--previous-state-directory");
    Configuration currConfig =
        StreamlinerUtil.createConfig(
            stateDirectory, StreamlinerUtil.readYamlFile(config), "--state-directory");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(
        diff, String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));

    outputDir =
        String.format("%s/%s", outputDir, "testScriptCommandForSchemaEvolution_no_schema_changes");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config, stateDirectory, previousDirectory, typeMapping, templateDirectory, outputDir);

    File f = new File(String.format("%s/Makefile", outputDir));
    assertTrue(f.exists());
    assertTrue(f.isFile());
  }

  @Test
  public void testScriptCommandForSchemaEvolution_table_deleted() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("require investigation");
    String stateDirectory = "src/test/resources/configDiff/deleteTable/state-directory";
    String previousDirectory = "src/test/resources/configDiff/deleteTable/previous-state-directory";
    Configuration prevConfig =
        StreamlinerUtil.createConfig(
            previousDirectory, StreamlinerUtil.readYamlFile(config), "--previous-state-directory");
    Configuration currConfig =
        StreamlinerUtil.createConfig(
            stateDirectory, StreamlinerUtil.readYamlFile(config), "--state-directory");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(
        diff, String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));

    outputDir =
        String.format("%s/%s", outputDir, "testScriptCommandForSchemaEvolution_table_deleted");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config, stateDirectory, previousDirectory, typeMapping, templateDirectory, outputDir);
  }
}
