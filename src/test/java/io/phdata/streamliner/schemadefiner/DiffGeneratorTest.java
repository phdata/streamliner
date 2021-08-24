package io.phdata.streamliner.schemadefiner;

import io.phdata.streamliner.schemadefiner.configbuilder.DiffGenerator;
import io.phdata.streamliner.schemadefiner.model.ColumnDiff;
import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.ConfigurationDiff;
import io.phdata.streamliner.schemadefiner.model.TableDiff;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class DiffGeneratorTest {
  private static  String outputPath = "src/test/output/";
  private String outputFile = "src/test/output/configDiff/streamliner-configDiff.yml";

  @Before
  public void before() {
    StreamlinerUtil.deleteDirectory(new File(outputPath));
  }

  // Please comment this after() if want to see the generated output files.
  @AfterClass
  public static void after() {
    StreamlinerUtil.deleteDirectory(new File(outputPath));
  }

  @Test
  public void testPrevConfigEmpty(){
    Configuration prevConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addAllColumn/prevConfig.yml");
    Configuration currConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addAllColumn/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputFile);
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(outputFile);

    assertNotNull(configDiff);
    assertNull(configDiff.getPreviousDestination());
    assertNotNull(configDiff.getCurrentDestination());
    assertNotNull(configDiff.getTableDiffs());
    assertFalse(configDiff.getTableDiffs().isEmpty());
    assertEquals(1, configDiff.getTableDiffs().size());

    TableDiff tableDiff = configDiff.getTableDiffs().get(0);
    assertEquals("Employee", tableDiff.getDestinationName());
    assertEquals(false, tableDiff.isExistsInDestination());
    assertEquals(true, tableDiff.isExistsInSource());
    assertEquals(1, tableDiff.getColumnDiffs().size());

    ColumnDiff colDiff = tableDiff.getColumnDiffs().get(0);
    assertNull(colDiff.getPreviousColumnDef());
    assertNotNull(colDiff.getCurrentColumnDef());
    assertEquals("ID", colDiff.getCurrentColumnDef().getSourceName());
    assertEquals("NUMBER", colDiff.getCurrentColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), colDiff.getCurrentColumnDef().getPrecision());
    assertTrue(colDiff.getIsAdd());
    assertFalse(colDiff.getIsDeleted());
    assertFalse(colDiff.getIsUpdate());
  }

  @Test
  public void testNewColumnAdded(){
    Configuration prevConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addColumn/prevConfig.yml");
    Configuration currConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addColumn/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputFile);
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(outputFile);
    assertNotNull(configDiff);
    assertNotNull(configDiff.getPreviousDestination());
    assertNotNull(configDiff.getCurrentDestination());
    assertNotNull(configDiff.getTableDiffs());
    assertFalse(configDiff.getTableDiffs().isEmpty());
    assertEquals(1, configDiff.getTableDiffs().size());

    TableDiff tableDiff = configDiff.getTableDiffs().get(0);
    assertEquals("Employee", tableDiff.getDestinationName());
    assertTrue(tableDiff.isExistsInDestination());
    assertTrue(tableDiff.isExistsInSource());
    assertEquals(1, tableDiff.getColumnDiffs().size());

    ColumnDiff colDiff = tableDiff.getColumnDiffs().get(0);
    assertNull(colDiff.getPreviousColumnDef());
    assertNotNull(colDiff.getCurrentColumnDef());
    assertEquals("Age", colDiff.getCurrentColumnDef().getSourceName());
    assertEquals("Number", colDiff.getCurrentColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), colDiff.getCurrentColumnDef().getPrecision());
    assertTrue(colDiff.getIsAdd());
    assertFalse(colDiff.getIsDeleted());
    assertFalse(colDiff.getIsUpdate());
  }

  @Test
  public void testColumnChanged(){
    Configuration prevConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/changeColumn/prevConfig.yml");
    Configuration currConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/changeColumn/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputFile);
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(outputFile);
    assertNotNull(configDiff);
    assertNotNull(configDiff.getPreviousDestination());
    assertNotNull(configDiff.getCurrentDestination());
    assertNotNull(configDiff.getTableDiffs());
    assertFalse(configDiff.getTableDiffs().isEmpty());
    assertEquals(1, configDiff.getTableDiffs().size());

    TableDiff tableDiff = configDiff.getTableDiffs().get(0);
    assertEquals("Employee", tableDiff.getDestinationName());
    assertTrue(tableDiff.isExistsInDestination());
    assertTrue(tableDiff.isExistsInSource());
    assertEquals(1, tableDiff.getColumnDiffs().size());

    ColumnDiff colDiff = tableDiff.getColumnDiffs().get(0);
    assertNotNull(colDiff.getPreviousColumnDef());
    assertEquals("NAME", colDiff.getPreviousColumnDef().getSourceName());
    assertEquals("VARCHAR", colDiff.getPreviousColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), colDiff.getPreviousColumnDef().getPrecision());

    assertNotNull(colDiff.getCurrentColumnDef());
    assertEquals("NAME", colDiff.getCurrentColumnDef().getSourceName());
    assertEquals("VARCHAR2", colDiff.getCurrentColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), colDiff.getCurrentColumnDef().getPrecision());
    assertFalse(colDiff.getIsAdd());
    assertFalse(colDiff.getIsDeleted());
    assertTrue(colDiff.getIsUpdate());
  }

  @Test
  public void testColumnDeleted(){
    Configuration prevConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/deleteColumn/prevConfig.yml");
    Configuration currConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/deleteColumn/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputFile);
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(outputFile);
    assertNotNull(configDiff);
    assertNotNull(configDiff.getPreviousDestination());
    assertNotNull(configDiff.getCurrentDestination());
    assertNotNull(configDiff.getTableDiffs());
    assertFalse(configDiff.getTableDiffs().isEmpty());
    assertEquals(1, configDiff.getTableDiffs().size());

    TableDiff tableDiff = configDiff.getTableDiffs().get(0);
    assertEquals("Employee", tableDiff.getDestinationName());
    assertTrue(tableDiff.isExistsInDestination());
    assertTrue(tableDiff.isExistsInSource());
    assertEquals(1, tableDiff.getColumnDiffs().size());

    ColumnDiff colDiff = tableDiff.getColumnDiffs().get(0);
    assertNotNull(colDiff.getPreviousColumnDef());
    assertNull(colDiff.getCurrentColumnDef());
    assertEquals("ID", colDiff.getPreviousColumnDef().getSourceName());
    assertEquals("NUMBER", colDiff.getPreviousColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), colDiff.getPreviousColumnDef().getPrecision());
    assertFalse(colDiff.getIsAdd());
    assertTrue(colDiff.getIsDeleted());
    assertFalse(colDiff.getIsUpdate());
  }

  @Test
  public void testColumnDeletedAddedChanged(){
    Configuration prevConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/hybridChanges/prevConfig.yml");
    Configuration currConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/hybridChanges/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputFile);
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(outputFile);
    assertNotNull(configDiff);
    assertNotNull(configDiff.getPreviousDestination());
    assertNotNull(configDiff.getCurrentDestination());
    assertNotNull(configDiff.getTableDiffs());
    assertFalse(configDiff.getTableDiffs().isEmpty());
    assertEquals(1, configDiff.getTableDiffs().size());

    TableDiff tableDiff = configDiff.getTableDiffs().get(0);
    assertEquals("Employee", tableDiff.getDestinationName());
    assertTrue(tableDiff.isExistsInDestination());
    assertTrue(tableDiff.isExistsInSource());
    assertEquals(3, tableDiff.getColumnDiffs().size());

    ColumnDiff addedColDiff =
        tableDiff.getColumnDiffs().stream()
            .filter(colDiff -> colDiff.getIsAdd())
            .collect(Collectors.toList())
            .get(0);
    assertNull(addedColDiff.getPreviousColumnDef());
    assertNotNull(addedColDiff.getCurrentColumnDef());
    assertEquals("Salary", addedColDiff.getCurrentColumnDef().getSourceName());
    assertEquals("Number", addedColDiff.getCurrentColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), addedColDiff.getCurrentColumnDef().getPrecision());
    assertTrue(addedColDiff.getIsAdd());
    assertFalse(addedColDiff.getIsDeleted());
    assertFalse(addedColDiff.getIsUpdate());

    ColumnDiff deletedColDiff =
        tableDiff.getColumnDiffs().stream()
            .filter(colDiff -> colDiff.getIsDeleted())
            .collect(Collectors.toList())
            .get(0);
    assertNotNull(deletedColDiff.getPreviousColumnDef());
    assertNull(deletedColDiff.getCurrentColumnDef());
    assertEquals("Age", deletedColDiff.getPreviousColumnDef().getSourceName());
    assertEquals("Number", deletedColDiff.getPreviousColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), deletedColDiff.getPreviousColumnDef().getPrecision());
    assertFalse(deletedColDiff.getIsAdd());
    assertTrue(deletedColDiff.getIsDeleted());
    assertFalse(deletedColDiff.getIsUpdate());

    ColumnDiff changedColDiff =
        tableDiff.getColumnDiffs().stream()
            .filter(colDiff -> colDiff.getIsUpdate())
            .collect(Collectors.toList())
            .get(0);
    assertNotNull(changedColDiff.getPreviousColumnDef());
    assertEquals("NAME", changedColDiff.getPreviousColumnDef().getSourceName());
    assertEquals("VARCHAR", changedColDiff.getPreviousColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), changedColDiff.getPreviousColumnDef().getPrecision());

    assertNotNull(changedColDiff.getCurrentColumnDef());
    assertEquals("NAME", changedColDiff.getCurrentColumnDef().getSourceName());
    assertEquals("VARCHAR2", changedColDiff.getCurrentColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), changedColDiff.getCurrentColumnDef().getPrecision());

    assertFalse(changedColDiff.getIsAdd());
    assertFalse(changedColDiff.getIsDeleted());
    assertTrue(changedColDiff.getIsUpdate());
  }

  @Test
  public void testTableDelete(){
    Configuration prevConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/deleteTable/prevConfig.yml");
    Configuration currConfig =
        StreamlinerUtil.readYamlFile("src/test/resources/configDiff/deleteTable/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputFile);
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(outputFile);

    assertNotNull(configDiff);
    assertNotNull(configDiff.getPreviousDestination());
    assertNotNull(configDiff.getCurrentDestination());
    assertNotNull(configDiff.getTableDiffs());
    assertFalse(configDiff.getTableDiffs().isEmpty());
    assertEquals(1, configDiff.getTableDiffs().size());

    TableDiff tableDiff = configDiff.getTableDiffs().get(0);
    assertEquals("Department", tableDiff.getDestinationName());
    assertTrue(tableDiff.isExistsInDestination());
    assertFalse(tableDiff.isExistsInSource());
    assertEquals(1, tableDiff.getColumnDiffs().size());

    ColumnDiff colDiff = tableDiff.getColumnDiffs().get(0);
    assertNotNull(colDiff.getPreviousColumnDef());
    assertNull(colDiff.getCurrentColumnDef());
    assertEquals("DEPT_ID", colDiff.getPreviousColumnDef().getSourceName());
    assertEquals("NUMBER", colDiff.getPreviousColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), colDiff.getPreviousColumnDef().getPrecision());
    assertFalse(colDiff.getIsAdd());
    assertTrue(colDiff.getIsDeleted());
    assertFalse(colDiff.getIsUpdate());
    }

  @Test
  public void testTableAdded(){
    Configuration prevConfig =
            StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addTable/prevConfig.yml");
    Configuration currConfig =
            StreamlinerUtil.readYamlFile("src/test/resources/configDiff/addTable/currConfig.yml");
    ConfigurationDiff diff = DiffGenerator.createConfigDiff(prevConfig, currConfig);
    StreamlinerUtil.writeConfigToYaml(diff, outputFile);
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(outputFile);

    assertNotNull(configDiff);
    assertNotNull(configDiff.getPreviousDestination());
    assertNotNull(configDiff.getCurrentDestination());
    assertNotNull(configDiff.getTableDiffs());
    assertFalse(configDiff.getTableDiffs().isEmpty());
    assertEquals(1, configDiff.getTableDiffs().size());

    TableDiff tableDiff = configDiff.getTableDiffs().get(0);
    assertEquals("Department", tableDiff.getDestinationName());
    assertFalse(tableDiff.isExistsInDestination());
    assertTrue(tableDiff.isExistsInSource());
    assertEquals(1, tableDiff.getColumnDiffs().size());

    ColumnDiff colDiff = tableDiff.getColumnDiffs().get(0);
    assertNull(colDiff.getPreviousColumnDef());
    assertNotNull(colDiff.getCurrentColumnDef());
    assertEquals("DEPT_ID", colDiff.getCurrentColumnDef().getSourceName());
    assertEquals("NUMBER", colDiff.getCurrentColumnDef().getDataType());
    assertEquals(Integer.valueOf(38), colDiff.getCurrentColumnDef().getPrecision());
    assertTrue(colDiff.getIsAdd());
    assertFalse(colDiff.getIsDeleted());
    assertFalse(colDiff.getIsUpdate());
  }
}
