package io.phdata.streamliner.schemadefiner;

import io.phdata.streamliner.schemadefiner.ConfigBuilder.DiffGenerator;
import io.phdata.streamliner.schemadefiner.ConfigBuilder.ScriptCommand;
import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.ConfigurationDiff;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class ScriptCommandTest {
  private static String outputPath = "src/test/output/";
  String templateDirectory = "src/main/resources/templates/snowflake";
  String typeMapping = "src/main/resources/type-mapping.yml";
  String outputDir = "src/test/resources/results/scriptCommand";
  String configDiff = "src/test/output/confDiff/streamliner-configuration-diff.yml";

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Before
  public void before() {
    StreamlinerUtil.deleteDirectory(new File(outputPath));
  }

  // Please comment this after() if want to see the generated streamliner-configuration-diff.yml files.
  @AfterClass
  public static void after() {
    StreamlinerUtil.deleteDirectory(new File(outputPath));
  }

  @Test
  public void testScriptCommandForSchemaEvolution_Alter_Table_Add_column() throws IOException {
    Configuration prevConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addColumn/prevConfig.yml");
    Configuration currConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addColumn/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputPath);

    outputDir =
        String.format("%s/%s", outputDir, "testScriptCommandForSchemaEvolution_Alter_Table_Add_column");
    StreamlinerUtil.createDir(outputDir);
    ScriptCommand.build(null, configDiff, typeMapping, templateDirectory, outputDir);
    File f = new File(String.format("%s/Employee", outputDir));
    assertTrue(f.exists());
    assertTrue(f.isDirectory());

    Arrays.stream(f.listFiles())
        .forEach(
            file -> {
              assertTrue(
                  file.getName().equals("evolve-schema.sql") || file.getName().equals("Makefile"));
            });
  }

  @Test
  public void testScriptCommandForSchemaEvolution_Alter_Table_Modify_column() throws IOException {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("require investigation");
    Configuration prevConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/changeColumn/prevConfig.yml");
    Configuration currConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/changeColumn/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputPath);

    outputDir =
        String.format("%s/%s", outputDir, "testScriptCommandForSchemaEvolution_Alter_Table_Modify_column");
    StreamlinerUtil.createDir(outputDir);
    ScriptCommand.build(null, configDiff, typeMapping, templateDirectory, outputDir);
  }

  @Test
  public void testScriptCommandForSchemaEvolution_Create_Table() throws IOException {
    Configuration prevConfig =
            StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addTable/prevConfig.yml");
    Configuration currConfig =
            StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addTable/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputPath);

    outputDir =
            String.format("%s/%s", outputDir, "testScriptCommandForSchemaEvolution_Create_Table");
    StreamlinerUtil.createDir(outputDir);
    ScriptCommand.build(null, configDiff, typeMapping, templateDirectory, outputDir);
    File f = new File(String.format("%s/Department", outputDir));
    assertTrue(f.exists());
    assertTrue(f.isDirectory());

    Arrays.stream(f.listFiles())
            .forEach(
                    file -> {
                      assertTrue(
                              file.getName().equals("evolve-schema.sql") || file.getName().equals("Makefile"));
                    });
  }
}
