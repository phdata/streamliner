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

import static org.junit.Assert.*;

import io.phdata.streamliner.App;
import io.phdata.streamliner.schemadefiner.JdbcCrawler;
import io.phdata.streamliner.schemadefiner.SchemaDefiner;
import io.phdata.streamliner.schemadefiner.StreamlinerConfigReader;
import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.shaded.org.yaml.snakeyaml.Yaml;
import org.testcontainers.utility.DockerImageName;
import schemacrawler.crawl.StreamlinerCatalog;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MySQLTestContainerTest {

  private static final DockerImageName MYSQL_57_IMAGE = DockerImageName.parse("mysql:5.7.34");
  private static final String STREAMLINER_DATABASE_NAME = "STREAMLINER_DB";
  private static final String STREAMLINER_DATABASE_USERNAME = "streamliner_user";
  private static final String STREAMLINER_DATABASE_PASSWORD = "streamliner_pwd";
  private static final String SCHEMA_COMMAND_OUTPUT_PATH =
      "src/test/output/conf/streamliner-configuration.yml";
  private String templateDirectory = "src/test/resources/templates/snowflake";
  private String typeMapping = "src/main/resources/type-mapping.yml";
  private String outputDir = "src/test/resources/results/schemaScriptCommand";
  private static Connection con = null;
  private Yaml yaml = new Yaml();

  @ClassRule
  public static MySQLContainer mysql =
      new MySQLContainer(MYSQL_57_IMAGE)
          .withDatabaseName(STREAMLINER_DATABASE_NAME)
          .withUsername(STREAMLINER_DATABASE_USERNAME)
          .withPassword(STREAMLINER_DATABASE_PASSWORD);

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @BeforeClass
  public static void before() throws SQLException {
    mysql.start();
    con =
        StreamlinerUtil.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    performExecuteUpdate(
        con,
        "CREATE TABLE Persons (\n"
            + "    PersonID int NOT NULL,\n"
            + "    LastName varchar(255),\n"
            + "    FirstName varchar(255),\n"
            + "    Address varchar(255),\n"
            + "    City varchar(255)\n"
            + ");");
  }

  @AfterClass
  public static void after() throws SQLException {
    mysql.stop();
    con.close();
  }

  @Test
  public void test03JdbcCrawler() {
    List<String> tableTypes = new ArrayList<>();
    tableTypes.add("table");
    Jdbc jdbc =
        new Jdbc(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getDatabaseName(), tableTypes);
    SchemaDefiner definer = new JdbcCrawler(jdbc, mysql.getPassword());

    // Mysql testcontainer jdbcCrawler
    StreamlinerCatalog catalog = definer.retrieveSchema();

    assertNotNull(catalog);
    assertFalse(catalog.getSchemas().isEmpty());

    List<Schema> schemaList = (List<Schema>) catalog.getSchemas();
    schemaList.stream()
        .forEach(
            schema -> {
              if (schema.getCatalogName().equals(STREAMLINER_DATABASE_NAME)) {
                Table table = ((List<Table>) catalog.getTables(schema)).get(0);
                assertFalse(catalog.getTables(schema).isEmpty());
                assertEquals(5, table.getColumns().size());
                assertTrue(table.getName().equals("Persons"));
                assertEquals(STREAMLINER_DATABASE_NAME, table.getSchema().getCatalogName());
              }
            });

    assertNotNull(catalog.getDriverClassName());
    assertEquals(mysql.getDriverClassName(), catalog.getDriverClassName());
  }

  // @Test
  public void test05StreamlinerConfigReader() {
    // reading config file generated after schema command and converting it to StreamlinerCatalog
    SchemaDefiner definer = new StreamlinerConfigReader(SCHEMA_COMMAND_OUTPUT_PATH);
    StreamlinerCatalog catalog = definer.retrieveSchema();

    assertNotNull(catalog);
    assertNotNull(catalog.getSchemas());
    List<Schema> schemaList = (List<Schema>) catalog.getSchemas();
    schemaList.stream()
        .forEach(
            schema -> {
              if (schema.getCatalogName().equals(STREAMLINER_DATABASE_NAME)) {
                Table table = ((List<Table>) catalog.getTables(schema)).get(0);
                assertFalse(catalog.getTables(schema).isEmpty());
                assertEquals(5, table.getColumns().size());
                assertTrue(table.getName().equals("Persons"));
                assertEquals(STREAMLINER_DATABASE_NAME, table.getSchema().getCatalogName());
              }
            });
    assertNotNull(catalog.getDriverClassName());
    assertEquals(mysql.getDriverClassName(), catalog.getDriverClassName());
  }

  @Test
  public void test04SchemaCommand_new() throws Exception {
    String config = "src/test/resources/conf/ingest-configuration.yml";
    String stateDirectory =
        "src/test/resources/results/schemaCommand/test04SchemaCommand_new/state-directory";
    String dbPass = mysql.getPassword();
    boolean createDocs = false;
    updateSourceDetail(config);
    // schema command with new implementations
    SchemaCommand.build(config, stateDirectory, dbPass, createDocs, null);

    Configuration tableConfig =
        StreamlinerUtil.readYamlFile(String.format("%s/%s", stateDirectory, "Persons.yml"));
    assertNotNull(tableConfig);
    assertNotNull(tableConfig.getTables());
    assertEquals(1, tableConfig.getTables().size());

    TableDefinition tableDef = tableConfig.getTables().get(0);
    assertEquals("Snowflake", tableDef.getType());
    assertEquals("Persons", tableDef.getSourceName());
    assertFalse(tableDef.getColumns().isEmpty());

    List<ColumnDefinition> colDef =
        tableDef.getColumns().stream()
            .filter(c -> c.getSourceName().equals("PersonID"))
            .collect(Collectors.toList());
    assertEquals(false, colDef.get(0).isNullable());
  }

  @Test
  public void test06ConfigurationDiff_serialize() {
    String outputFile = "src/test/output/confDiff/streamliner-configuration-diff.yml";
    String configPath1 = "src/test/resources/conf/ingest-configuration.yml";
    String configPath2 = "src/test/resources/conf/ingest-configuration-glue.yml";

    // reading previous destination config
    Configuration conf1 = StreamlinerUtil.readYamlFile(configPath1);
    // reading current destination config
    Configuration conf2 = StreamlinerUtil.readYamlFile(configPath2);

    ColumnDefinition prevColDef1 =
        new ColumnDefinition("ID", "ID", "Number", "Emp Id", 255, 0, true);
    ColumnDefinition currColDef1 =
        new ColumnDefinition("Emp_Id", "Emp_Id", "Number", "Employee Id", 255, 0, true);
    ColumnDiff columnDiff1 = new ColumnDiff(prevColDef1, currColDef1, false, false, true);

    ColumnDefinition prevColDef2 =
        new ColumnDefinition("Name", "Name", "Varchar2", "Emp Name", 255, 0, true);
    ColumnDefinition currColDef2 =
        new ColumnDefinition("Emp_Name", "Emp_Name", "Varchar2", "Employee Name", 255, 0, true);
    ColumnDiff columnDiff2 = new ColumnDiff(prevColDef2, currColDef2, false, false, true);

    List<ColumnDiff> colList = new ArrayList<>();
    colList.add(columnDiff1);
    colList.add(columnDiff2);
    TableDiff tableDiff1 = new TableDiff("Snowflake", "Employee", "Employee", colList, true, true);

    colList = new ArrayList<>();
    colList.add(columnDiff1);
    TableDiff tableDiff2 = new TableDiff("Snowflake", "Emp", "Emp", colList, true, true);

    List<TableDiff> tableList = new ArrayList<>();
    tableList.add(tableDiff1);
    tableList.add(tableDiff2);

    ConfigurationDiff configDiff =
        new ConfigurationDiff(
            "STREAMLINER_QUICKSTART_1",
            "SANDBOX",
            "snowflake-snowpipe-append",
            conf1.getDestination(),
            conf2.getDestination(),
            tableList);

    StreamlinerUtil.deleteDirectory(new File("src/test/output"));
    StreamlinerUtil.writeConfigToYaml(configDiff, outputFile);
  }

  @Test
  public void test07ConfigurationDiff_deserialize() {
    String configDiffPath = "src/test/output/confDiff/streamliner-configuration-diff.yml";
    // reading config diff yaml file.
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(configDiffPath);

    assertEquals("STREAMLINER_QUICKSTART_1", configDiff.getName());
    assertEquals("SANDBOX", configDiff.getEnvironment());
    assertEquals("snowflake-snowpipe-append", configDiff.getPipeline());

    assertNotNull(configDiff.getPreviousDestination());
    Snowflake prevDestination = (Snowflake) configDiff.getPreviousDestination();
    Snowflake currDestination = (Snowflake) configDiff.getCurrentDestination();
    assertEquals("snowsql -c connection", prevDestination.getSnowSqlCommand());
    assertEquals("s3://streamliner-quickstart-1/employees/", prevDestination.getStoragePath());

    assertNotNull(configDiff.getCurrentDestination());
    assertEquals("snowsql -c streamliner_admin", currDestination.getSnowSqlCommand());
    assertEquals(
        "s3://phdata-snowflake-stage/data/phdata-task/HR/", currDestination.getStoragePath());

    assertNotNull(configDiff.getTableDiffs());
    assertFalse(configDiff.getTableDiffs().isEmpty());
    assertEquals(configDiff.getTableDiffs().size(), 2);

    configDiff.getTableDiffs().stream()
        .forEach(
            tableDiff -> {
              if (tableDiff.getDestinationName().equals("Employee")) {
                assertEquals(2, tableDiff.getColumnDiffs().size());
              } else if (tableDiff.getDestinationName().equals("Emp")) {
                assertEquals(1, tableDiff.getColumnDiffs().size());
              }
            });
  }

  @Test
  public void test08_deserialize_serialize() {
    String[] configList = {
      "src/test/resources/scalaConf/glue/snowflake/ingest-configuration.yml",
      "src/test/resources/scalaConf/glue/snowflake/streamliner-configuration.yml",
      "src/test/resources/scalaConf/jdbc/snowflake/ingest-configuration.yml",
      "src/test/resources/scalaConf/jdbc/snowflake/streamliner-configuration.yml"
    };
    Arrays.asList(configList).stream()
        .forEach(
            inputConfig -> {
              try {
                deserialize_serialize(inputConfig);
              } catch (IOException e) {
                e.printStackTrace();
              }
            });

    Configuration config1 =
        StreamlinerUtil.readYamlFile(
            "src/test/resources/scalaConf/glue/snowflake/ingest-configuration.yml");
    Configuration config2 =
        StreamlinerUtil.readYamlFile(
            "src/test/resources/scalaConf/glue/snowflake/streamliner-configuration.yml");
    assertFalse(config1.equals(config2));
  }

  @Test
  public void test09ScalaAppMainMethod_usingNewSchemaCommand_() throws Exception {
    String config = "src/test/resources/conf/ingest-configuration.yml";
    String stateDirectory =
        "src/test/resources/results/schemaCommand/test09ScalaAppMainMethod_usingNewSchemaCommand_/state-directory";
    updateSourceDetail(config);

    // --create-docs is optional
    String schemaCommand1[] = {
      "schema",
      "--config",
      config,
      "--state-directory",
      stateDirectory,
      "--database-password",
      mysql.getPassword()
    };
    testSchemaCommandOptionalParams(stateDirectory, schemaCommand1);
    StreamlinerUtil.deleteDirectory(new File("src/test/output"));
  }

  @Test
  public void test10SchemaCommandOptionalPassword() {
    /* --database-password is optional parameter. But if source type is Jdbc then password is mandatory */
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("A databasePassword is required when crawling JDBC source");
    String config = "src/test/resources/conf/ingest-configuration.yml";
    String stateDirectory =
        "src/test/resources/results/schemaCommand/test10SchemaCommandOptionalPassword/state-directory";
    // --database-password is optional
    String schemaCommand[] = {"schema", "--config", config, "--state-directory", stateDirectory};
    // Scala App main method
    App.main(schemaCommand);
  }

  @Test
  public void test11SchemaCommandForSchemaEvolution() throws Exception {
    String config = "src/test/resources/conf/ingest-configuration.yml";
    String stateDirectory =
        "src/test/resources/results/schemaCommand/test11SchemaCommandForSchemaEvolution/state-directory";
    String previousStateDirectory =
        "src/test/resources/results/schemaCommand/test11SchemaCommandForSchemaEvolution/previous-state-directory";
    String dbPass = mysql.getPassword();
    boolean createDocs = false;

    updateSourceDetail(config);

    // Schema command
    SchemaCommand.build(config, previousStateDirectory, dbPass, createDocs, null);

    // added a column
    performExecute(con, "ALTER TABLE Persons ADD Age VARCHAR(40) NOT NULL;");

    // schema command for schema evolution
    SchemaCommand.build(config, stateDirectory, dbPass, createDocs, previousStateDirectory);

    ConfigurationDiff diff =
        StreamlinerUtil.readConfigDiffFromPath(
            String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));
    assertNotNull(diff);
    assertNotNull(diff.getTableDiffs());
    assertEquals(1, diff.getTableDiffs().size());

    TableDiff tableDiff = diff.getTableDiffs().get(0);
    assertEquals("Persons", tableDiff.getDestinationName());
    assertTrue(tableDiff.existsInDestination);
    assertTrue(tableDiff.existsInSource);
    assertNotNull(tableDiff.getColumnDiffs());
    assertEquals(1, tableDiff.getColumnDiffs().size());

    ColumnDiff colDiff = tableDiff.getColumnDiffs().get(0);
    assertNull(colDiff.getPreviousColumnDef());
    assertNotNull(colDiff.getCurrentColumnDef());

    ColumnDefinition colDef = colDiff.getCurrentColumnDef();
    assertEquals("Age", colDef.getSourceName());
    assertEquals("VARCHAR", colDef.getDataType());

    assertTrue(colDiff.getIsAdd());

    StreamlinerUtil.deleteDirectory(new File("src/test/output"));
  }

  @Test
  public void test12TableWhiteListingTest() throws SQLException, FileNotFoundException {
    performExecuteUpdate(
        con,
        "CREATE TABLE Person2 (\n"
            + "    PersonID int NOT NULL,\n"
            + "    LastName varchar(255),\n"
            + "    FirstName varchar(255),\n"
            + "    Address varchar(255),\n"
            + "    City varchar(255)\n"
            + ");");
    String config = "src/test/resources/conf/ingest-configuration-tableWhitelisting.yml";
    String stateDirectory =
        "src/test/resources/results/schemaCommand/test12TableWhiteListingTest/state-directory";
    String dbPass = mysql.getPassword();
    boolean createDocs = false;

    updateSourceDetail(config);

    // Schema command
    SchemaCommand.build(config, stateDirectory, dbPass, createDocs, null);

    Configuration outputConfig =
        StreamlinerUtil.readYamlFile(String.format("%s/%s", stateDirectory, "Persons.yml"));

    assertNotNull(outputConfig.getTables());
    assertEquals(1, outputConfig.getTables().size());
    assertNotNull(outputConfig.getTables().get(0).getPrimaryKeys());
    assertEquals(2, outputConfig.getTables().get(0).getPrimaryKeys().size());
    assertEquals("Persons", outputConfig.getTables().get(0).getSourceName());

    StreamlinerUtil.deleteDirectory(new File("src/test/output"));
  }

  @Test
  public void test13TableIgnoreTest() throws SQLException, FileNotFoundException {
    performExecuteUpdate(
        con,
        "CREATE TABLE Employee (\n"
            + "    EmployeeId int NOT NULL,\n"
            + "    LastName varchar(255),\n"
            + "    FirstName varchar(255),\n"
            + "    Address varchar(255),\n"
            + "    City varchar(255)\n"
            + ");");
    performExecuteUpdate(
        con,
        "CREATE TABLE Doctors (\n"
            + "    Id int NOT NULL,\n"
            + "    LastName varchar(255),\n"
            + "    FirstName varchar(255),\n"
            + "    Address varchar(255),\n"
            + "    City varchar(255)\n"
            + ");");
    String config = "src/test/resources/conf/ingest-configuration-tableIgnore.yml";
    String stateDirectory =
        "src/test/resources/results/schemaCommand/test13TableIgnoreTest/state-directory";
    String dbPass = mysql.getPassword();
    boolean createDocs = false;

    updateSourceDetail(config);

    // Schema command
    SchemaCommand.build(config, stateDirectory, dbPass, createDocs, null);

    Configuration outputConfig =
        StreamlinerUtil.readYamlFile(String.format("%s/%s", stateDirectory, "Persons.yml"));

    assertNotNull(outputConfig.getTables());
    assertEquals(1, outputConfig.getTables().size());
    assertNotNull(outputConfig.getTables().get(0).getPrimaryKeys());
    assertEquals(2, outputConfig.getTables().get(0).getPrimaryKeys().size());
    assertEquals("Persons", outputConfig.getTables().get(0).getSourceName());

    StreamlinerUtil.deleteDirectory(new File("src/test/output"));
  }

  @Test
  public void test14TableNameStrategy_search_replace() throws SQLException, FileNotFoundException {
    performExecuteUpdate(
        con,
        "CREATE TABLE PHDATA_EMPLOYEE (\n"
            + "    EmployeeId int NOT NULL,\n"
            + "    LastName varchar(255),\n"
            + "    FirstName varchar(255),\n"
            + "    Address varchar(255),\n"
            + "    City varchar(255)\n"
            + ");");
    performExecuteUpdate(
        con,
        "CREATE TABLE PHDATA_DEPARTMENT (\n"
            + "    DeptId int NOT NULL,\n"
            + "    DeptName varchar(255),\n"
            + "    EmployeeCount int,\n"
            + "    DeptHead varchar(255),\n"
            + "    Building varchar(255)\n"
            + ");");

    String config =
        "src/test/resources/conf/ingest-configuration-tableNameStrategy-searchReplace.yml";
    String stateDirectory =
        String.format(
            "%s/%s", outputDir, "test14SourceDestinationDifferentTableName/state-directory");
    String previousStateDirectory =
        String.format(
            "%s/%s",
            outputDir, "test14SourceDestinationDifferentTableName/previous-state-directory");
    String dbPass = mysql.getPassword();
    boolean createDocs = false;

    updateSourceDetail(config);

    // Schema command
    SchemaCommand.build(config, stateDirectory, dbPass, createDocs, null);
    File f = new File(stateDirectory);
    assertTrue(f.exists());
    String expectedSourceTables[] = {"PHDATA_DEPARTMENT", "PHDATA_EMPLOYEE"};
    String expectedDestinationTables[] = {"DEPARTMENT", "EMPLOYEE"};
    Arrays.stream(f.listFiles())
        .forEach(
            file -> {
              Configuration conf = StreamlinerUtil.readYamlFile(file.toString());
              conf.getTables().stream()
                  .forEach(
                      table -> {
                        assertTrue(
                            Arrays.asList(expectedSourceTables).contains(table.getSourceName()));
                        assertTrue(
                            Arrays.asList(expectedDestinationTables)
                                .contains(table.getDestinationName()));
                      });
            });

    // script command
    outputDir =
        String.format("%s/%s", outputDir, "test14SourceDestinationDifferentTableName/scripts");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config,
            stateDirectory,
            previousStateDirectory,
            typeMapping,
            templateDirectory,
            outputDir);
    String[] expectedFolder = {"EMPLOYEE", "DEPARTMENT"};
    f = new File(outputDir);
    Arrays.stream(f.listFiles())
        .forEach(
            t -> {
              if (t.isDirectory()) {
                assertTrue(Arrays.asList(expectedFolder).contains(t.getName()));
              } else if (t.isFile()) {
                assertEquals("Makefile", t.getName());
              }
            });
  }

  @Test
  public void test15TableNameStrategy_search_replace_evolve_schema()
      throws FileNotFoundException, SQLException {
    performExecuteUpdate(
        con,
        "CREATE TABLE IF NOT EXISTS PHDATA_EMPLOYEE (\n"
            + "    EmployeeId int NOT NULL,\n"
            + "    LastName varchar(255),\n"
            + "    FirstName varchar(255),\n"
            + "    Address varchar(255),\n"
            + "    City varchar(255)\n"
            + ");");
    performExecuteUpdate(
        con,
        "CREATE TABLE IF NOT EXISTS PHDATA_DEPARTMENT (\n"
            + "    DeptId int NOT NULL,\n"
            + "    DeptName varchar(255),\n"
            + "    EmployeeCount int,\n"
            + "    DeptHead varchar(255),\n"
            + "    Building varchar(255)\n"
            + ");");
    String config =
        "src/test/resources/conf/ingest-configuration-tableNameStrategy-searchReplace.yml";
    String stateDirectory =
        String.format(
            "%s/%s",
            outputDir, "test15SourceDestinationDifferentTableName_evolve_schema/state-directory");
    String previousStateDirectory =
        String.format(
            "%s/%s",
            outputDir,
            "test15SourceDestinationDifferentTableName_evolve_schema/previous-state-directory");
    String dbPass = mysql.getPassword();
    boolean createDocs = false;

    updateSourceDetail(config);

    // Schema command
    SchemaCommand.build(config, previousStateDirectory, dbPass, createDocs, null);

    // added a column
    performExecute(con, "ALTER TABLE PHDATA_EMPLOYEE ADD Age VARCHAR(40) NOT NULL;");

    // schema command for schema evolution
    SchemaCommand.build(config, stateDirectory, dbPass, createDocs, previousStateDirectory);
    ConfigurationDiff diff =
        StreamlinerUtil.readConfigDiffFromPath(
            String.format("%s/%s", stateDirectory, Constants.STREAMLINER_DIFF_FILE.value()));

    assertNotNull(diff);
    assertNotNull(diff.getTableDiffs());
    assertEquals(1, diff.getTableDiffs().size());

    TableDiff tableDiff = diff.getTableDiffs().get(0);
    assertEquals("EMPLOYEE", tableDiff.getDestinationName());
    assertNotNull(tableDiff.getColumnDiffs());
    assertEquals(1, tableDiff.getColumnDiffs().size());

    ColumnDiff colDiff = tableDiff.getColumnDiffs().get(0);
    ColumnDefinition colDef = colDiff.getCurrentColumnDef();
    assertEquals("Age", colDef.getSourceName());
    assertEquals("VARCHAR", colDef.getDataType());

    assertTrue(colDiff.getIsAdd());

    // script command
    outputDir =
        String.format(
            "%s/%s", outputDir, "test15SourceDestinationDifferentTableName_evolve_schema/scripts");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config,
            stateDirectory,
            previousStateDirectory,
            typeMapping,
            templateDirectory,
            outputDir);
    String[] expectedFiles = {"Makefile", "delta-change-summary.txt"};
    File f = new File(outputDir);
    Arrays.stream(f.listFiles())
        .forEach(
            t -> {
              if (t.isDirectory()) {
                assertEquals("EMPLOYEE", t.getName());
              } else if (t.isFile()) {
                assertTrue(Arrays.asList(expectedFiles).contains(t.getName()));
              }
            });
  }

  @Test
  public void test16TableNameStrategy_addPostfix() throws FileNotFoundException {
    String config = "src/test/resources/conf/ingest-configuration-tableNameStrategy-addPostfix.yml";
    String stateDirectory =
        String.format("%s/%s", outputDir, "test16TableNameStrategy_addPostfix/state-directory");
    String previousStateDirectory =
        String.format(
            "%s/%s", outputDir, "test16TableNameStrategy_addPostfix/previous-state-directory");
    String dbPass = mysql.getPassword();
    boolean createDocs = false;

    updateSourceDetail(config);

    // Schema command
    SchemaCommand.build(config, stateDirectory, dbPass, createDocs, null);
    File f = new File(String.format("%s/%s", stateDirectory, "Persons.yml"));
    assertTrue(f.exists());

    Configuration conf = StreamlinerUtil.readYamlFile(f.toString());
    TableDefinition tableDef = conf.getTables().get(0);
    assertEquals("Persons", tableDef.getSourceName());
    assertEquals("Persons_PHDATA", tableDef.getDestinationName());

    // script command
    outputDir = String.format("%s/%s", outputDir, "test16TableNameStrategy_addPostfix/scripts");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config,
            stateDirectory,
            previousStateDirectory,
            typeMapping,
            templateDirectory,
            outputDir);
    f = new File(outputDir);

    Arrays.stream(f.listFiles())
        .forEach(
            t -> {
              if (t.isDirectory()) {
                assertEquals("Persons_PHDATA", t.getName());
              } else if (t.isFile()) {
                assertEquals("Makefile", t.getName());
              }
            });
  }

  @Test
  public void test17TableNameStrategy_addPrefix() throws FileNotFoundException {
    String config = "src/test/resources/conf/ingest-configuration-tableNameStrategy-addPrefix.yml";
    String stateDirectory =
        String.format("%s/%s", outputDir, "test17TableNameStrategy_addPrefix/state-directory");
    String previousStateDirectory =
        String.format(
            "%s/%s", outputDir, "test17TableNameStrategy_addPrefix/previous-state-directory");
    String dbPass = mysql.getPassword();
    boolean createDocs = false;

    updateSourceDetail(config);

    // Schema command
    SchemaCommand.build(config, stateDirectory, dbPass, createDocs, null);
    File f = new File(String.format("%s/%s", stateDirectory, "Persons.yml"));
    assertTrue(f.exists());

    Configuration conf = StreamlinerUtil.readYamlFile(f.toString());
    TableDefinition tableDef = conf.getTables().get(0);
    assertEquals("Persons", tableDef.getSourceName());
    assertEquals("PHDATA_Persons", tableDef.getDestinationName());

    // script command
    outputDir = String.format("%s/%s", outputDir, "test17TableNameStrategy_addPrefix/scripts");
    StreamlinerUtil.createDir(outputDir);
    new ScriptCommand()
        .build(
            config,
            stateDirectory,
            previousStateDirectory,
            typeMapping,
            templateDirectory,
            outputDir);
    f = new File(outputDir);

    Arrays.stream(f.listFiles())
        .forEach(
            t -> {
              if (t.isDirectory()) {
                assertEquals("PHDATA_Persons", t.getName());
              } else if (t.isFile()) {
                assertEquals("Makefile", t.getName());
              }
            });
  }

  private void testSchemaCommandOptionalParams(String stateDirectory, String[] schemaCommand1) {
    // Scala App main method
    App.main(schemaCommand1);

    File f = new File(String.format("%s/%s", stateDirectory, "Persons.yml"));
    assertTrue(f.exists());

    Configuration config = StreamlinerUtil.readYamlFile(f.getAbsolutePath());
    assertNotNull(config);
    assertNotNull(config.getTables());
    assertFalse(config.getTables().isEmpty());
    TableDefinition tableDef = config.getTables().get(0);
    assertEquals("Snowflake", tableDef.getType());
    assertEquals("Persons", tableDef.getSourceName());
    assertFalse(tableDef.getColumns().isEmpty());

    List<ColumnDefinition> colDef =
        tableDef.getColumns().stream()
            .filter(c -> c.getSourceName().equals("PersonID"))
            .collect(Collectors.toList());
    assertEquals(false, colDef.get(0).isNullable());
  }

  private void deserialize_serialize(String inputConfig) throws IOException {
    String outputPath = "src/test/output/";
    String outputConfig1 = "src/test/output/temp-config1.yml";
    String outputConfig2 = "src/test/output/temp-config2.yml";

    StreamlinerUtil.deleteDirectory(new File(outputPath));
    // deserialize
    Configuration config = StreamlinerUtil.readYamlFile(inputConfig);
    StreamlinerUtil.createDir(outputPath);
    // serialize
    StreamlinerUtil.writeYamlFile(config, outputConfig1);
    // deserialzie
    Configuration tempConfig = StreamlinerUtil.readYamlFile(outputConfig1);
    assertTrue(config.equals(tempConfig));

    // serialize
    StreamlinerUtil.writeYamlFile(tempConfig, outputConfig2);
    // deserialzie
    Configuration tempConfig2 = StreamlinerUtil.readYamlFile(outputConfig2);
    assertTrue(config.equals(tempConfig2));
    assertTrue(tempConfig.equals(tempConfig2));
  }

  private Map<String, Object> updateSourceDetail(String configPath) throws FileNotFoundException {
    InputStream inputStream = new FileInputStream(configPath);
    Map<String, Object> data = yaml.load(inputStream);
    Map<String, Object> source = (Map<String, Object>) data.get("source");
    source.put("url", mysql.getJdbcUrl());
    source.put("username", mysql.getUsername());
    source.put("schema", mysql.getDatabaseName());
    PrintWriter writer = new PrintWriter(configPath);
    yaml.dump(data, writer);
    return data;
  }

  private static int performExecuteUpdate(Connection con, String sql) throws SQLException {
    Statement statement = con.createStatement();
    int row = statement.executeUpdate(sql);
    return row;
  }

  private static void performExecute(Connection con, String sql) throws SQLException {
    Statement statement = con.createStatement();
    statement.execute(sql);
  }
}
