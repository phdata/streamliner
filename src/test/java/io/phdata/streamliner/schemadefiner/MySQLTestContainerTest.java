package io.phdata.streamliner.schemadefiner;

import io.phdata.streamliner.App;
import io.phdata.streamliner.schemadefiner.configbuilder.SchemaCommand;
import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.shaded.org.yaml.snakeyaml.Yaml;
import org.testcontainers.utility.DockerImageName;
import schemacrawler.crawl.StreamlinerCatalog;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MySQLTestContainerTest {

    private static final DockerImageName MYSQL_57_IMAGE = DockerImageName.parse("mysql:5.7.34");
    private static final String STREAMLINER_DATABASE_NAME = "STREAMLINER_DB";
    private static final String STREAMLINER_DATABASE_USERNAME = "streamliner_user";
    private static final String STREAMLINER_DATABASE_PASSWORD = "streamliner_pwd";
    private static final String SCHEMA_COMMAND_OUTPUT_PATH= "src/test/output/conf/streamliner-configuration.yml";
    private static Connection con = null;
    private Yaml yaml = new Yaml();

    @ClassRule
    public static MySQLContainer mysql = new MySQLContainer(MYSQL_57_IMAGE)
            .withDatabaseName(STREAMLINER_DATABASE_NAME)
            .withUsername(STREAMLINER_DATABASE_USERNAME)
            .withPassword(STREAMLINER_DATABASE_PASSWORD);

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void before() throws SQLException {
        mysql.start();
        con = StreamlinerUtil.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        performExecuteUpdate(con, "CREATE TABLE Persons (\n" +
                "    PersonID int NOT NULL,\n" +
                "    LastName varchar(255),\n" +
                "    FirstName varchar(255),\n" +
                "    Address varchar(255),\n" +
                "    City varchar(255)\n" +
                ");");
    }

    @AfterClass
    public static void after() throws SQLException {
        mysql.stop();
        con.close();
    }

    @Test
    public void test03JdbcCrawler(){
        List<String> tableTypes = new ArrayList<>();
        tableTypes.add("table");
        Jdbc jdbc = new Jdbc(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getDatabaseName(), tableTypes);
        SchemaDefiner definer = new JdbcCrawler(jdbc, mysql.getPassword());

        // Mysql testcontainer jdbcCrawler
        StreamlinerCatalog catalog = definer.retrieveSchema();

        assertNotNull(catalog);
        assertFalse(catalog.getSchemas().isEmpty());

        List<Schema> schemaList = (List<Schema>) catalog.getSchemas();
        schemaList.stream().forEach(schema -> {
            if(schema.getCatalogName().equals(STREAMLINER_DATABASE_NAME)) {
                Table table = ((List<Table>) catalog.getTables(schema)).get(0);
                assertFalse(catalog.getTables(schema).isEmpty());
                assertEquals(5,  table.getColumns().size());
                assertTrue(table.getName().equals("Persons"));
                assertEquals(STREAMLINER_DATABASE_NAME,table.getSchema().getCatalogName());
            }
        });

        assertNotNull(catalog.getDriverClassName());
        assertEquals(mysql.getDriverClassName(), catalog.getDriverClassName());
    }

    @Test
    public void test05StreamlinerConfigReader(){
        //reading config file generated after schema command and converting it to StreamlinerCatalog
        SchemaDefiner definer = new StreamlinerConfigReader(SCHEMA_COMMAND_OUTPUT_PATH);
        StreamlinerCatalog catalog = definer.retrieveSchema();

        assertNotNull(catalog);
        assertNotNull(catalog.getSchemas());
        List<Schema> schemaList = (List<Schema>) catalog.getSchemas();
        schemaList.stream().forEach(schema -> {
            if(schema.getCatalogName().equals(STREAMLINER_DATABASE_NAME)) {
                Table table = ((List<Table>) catalog.getTables(schema)).get(0);
                assertFalse(catalog.getTables(schema).isEmpty());
                assertEquals(5,  table.getColumns().size());
                assertTrue(table.getName().equals("Persons"));
                assertEquals(STREAMLINER_DATABASE_NAME,table.getSchema().getCatalogName());
            }
        });
        assertNotNull(catalog.getDriverClassName());
        assertEquals(mysql.getDriverClassName(), catalog.getDriverClassName());
    }

    @Test
    public void test04SchemaCommand_new() throws Exception {
        String config = "src/test/resources/conf/ingest-configuration.yml";
        String outputFile = "src/test/output/conf/streamliner-configuration.yml";
        String dbPass = mysql.getPassword();
        boolean createDocs = false;

        File generatedOutputFolder = new File(outputFile);
        StreamlinerUtil.deleteDirectory(generatedOutputFolder);

        Map<String, Object> ingestConfigFile = updateSourceDetail(config);
        // schema command with new implementations
        SchemaCommand.build(config,outputFile,dbPass,createDocs, null, null);

        File f = new File(outputFile);
        assertTrue(f.exists());
        assertTrue(f.isFile());

        InputStream inputStream = new FileInputStream(f);
        Map<String, Object> outputConfigFile = yaml.load(inputStream);

        assertEquals(ingestConfigFile.get("name"), outputConfigFile.get("name"));
        assertEquals(ingestConfigFile.get("pipeline"), outputConfigFile.get("pipeline"));

        assertEquals(((Map<String, Object>) ingestConfigFile.get("source")).get("schema"), ((Map<String, Object>) outputConfigFile.get("source")).get("schema"));
        assertEquals(((Map<String, Object>) ingestConfigFile.get("destination")).get("type"), ((Map<String, Object>) outputConfigFile.get("destination")).get("type"));

        Map<String, Object> table = ((List<Map<String, Object>>) outputConfigFile.get("tables")).get(0);
        assertEquals("Snowflake", table.get("type"));
        assertEquals("Persons", table.get("sourceName"));

        List<Map<String, Object>> columnList = ((List<Map<String, Object>>) table.get("columns"));
        assertFalse(columnList.isEmpty());

        Map<String, Object> personID = columnList.stream().filter(c -> "PersonID".equalsIgnoreCase((String) c.get("sourceName"))).findFirst().get();
        assertEquals(false, personID.get("nullable"));
    }

  @Test
  public void test06ConfigurationDiff_serialize()  {
    String outputFile = "src/test/output/confDiff/streamliner-configuration-diff.yml";
    String configPath1 = "src/test/resources/conf/ingest-configuration.yml";
    String configPath2 = "src/test/resources/conf/ingest-configuration-glue.yml";

    // reading previous destination config
    Configuration conf1 = StreamlinerUtil.readConfigFromPath(configPath1);
      // reading current destination config
    Configuration conf2 = StreamlinerUtil.readConfigFromPath(configPath2);

    ColumnDefinition prevColDef1 = new ColumnDefinition("ID", "ID", "Number", "Emp Id", 255, 0, true);
    ColumnDefinition currColDef1 = new ColumnDefinition("Emp_Id", "Emp_Id", "Number", "Employee Id", 255, 0, true);
    ColumnDiff columnDiff1 = new ColumnDiff(prevColDef1, currColDef1, false, false, true);

    ColumnDefinition prevColDef2 =
        new ColumnDefinition("Name", "Name", "Varchar2", "Emp Name", 255, 0, true);
    ColumnDefinition currColDef2 =
        new ColumnDefinition("Emp_Name", "Emp_Name", "Varchar2", "Employee Name", 255, 0, true);
    ColumnDiff columnDiff2 = new ColumnDiff(prevColDef2, currColDef2, false, false, true);

    List<ColumnDiff> colList = new ArrayList<>();
    colList.add(columnDiff1);
    colList.add(columnDiff2);
    TableDiff tableDiff1 = new TableDiff("Snowflake", "Employee", colList, true, true);

    colList = new ArrayList<>();
    colList.add(columnDiff1);
    TableDiff tableDiff2 = new TableDiff("Snowflake", "Emp", colList, true, true);

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
  public void test07ConfigurationDiff_deserialize(){
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
  public void test08_deserialize_serialize(){
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
        StreamlinerUtil.readConfigFromPath(
            "src/test/resources/scalaConf/glue/snowflake/ingest-configuration.yml");
    Configuration config2 =
        StreamlinerUtil.readConfigFromPath(
            "src/test/resources/scalaConf/glue/snowflake/streamliner-configuration.yml");
    assertFalse(config1.equals(config2));
  }

  @Test
  public void test09ScalaAppMainMethod_usingNewSchemaCommand_() throws Exception {
    String config = "src/test/resources/conf/ingest-configuration.yml";
    String outputFile = "src/test/output/conf/streamliner-configuration.yml";
    updateSourceDetail(config);

    // --create-docs is optional
    String schemaCommand1[] = {
      "schema",
      "--config",
      config,
      "--output-file",
      outputFile,
      "--database-password",
      mysql.getPassword()
    };
    testSchemaCommandOptionalParams(outputFile, schemaCommand1);
      StreamlinerUtil.deleteDirectory(new File("src/test/output"));
  }

  @Test
  public void test10SchemaCommandOptionalPassword() {
    /* --database-password is optional parameter. But if source type is Jdbc then password is mandatory */
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("A databasePassword is required when crawling JDBC source");
    String config = "src/test/resources/conf/ingest-configuration.yml";
    String outputFile = "src/test/output/conf/streamliner-configuration.yml";
    // --database-password is optional
    String schemaCommand[] = {"schema", "--config", config, "--output-file", outputFile};
    // Scala App main method
    App.main(schemaCommand);
      StreamlinerUtil.deleteDirectory(new File("src/test/output"));
  }

    @Test
    public void test11SchemaCommandForSchemaEvolution() throws Exception {
        String config = "src/test/resources/conf/ingest-configuration.yml";
        String outputPath = "src/test/output/conf/streamliner-configuration.yml";
        String diffOutputFile = "src/test/output/configDiff/streamliner-configDiff.yml";
        String dbPass = mysql.getPassword();
        boolean createDocs = false;

        File generatedOutputFolder = new File(outputPath);
        StreamlinerUtil.deleteDirectory(generatedOutputFolder);

        Map<String, Object> ingestConfigFile = updateSourceDetail(config);

        // Schema comnmand
        SchemaCommand.build(config,outputPath,dbPass,createDocs, null, null);

        // added a column
        performExecute(con, "ALTER TABLE Persons ADD Age VARCHAR(40) NOT NULL;");

        // schema command for schema evolution
        SchemaCommand.build(config,"src/test/output/conf/streamliner-config2.yml",dbPass,createDocs, outputPath, diffOutputFile);

        ConfigurationDiff diff = StreamlinerUtil.readConfigDiffFromPath(diffOutputFile);
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
        performExecuteUpdate(con, "CREATE TABLE Person2 (\n" +
                "    PersonID int NOT NULL,\n" +
                "    LastName varchar(255),\n" +
                "    FirstName varchar(255),\n" +
                "    Address varchar(255),\n" +
                "    City varchar(255)\n" +
                ");");
        String config = "src/test/resources/conf/ingest-configuration-tableWhitelisting.yml";
        String outputPath = "src/test/output/conf/streamliner-configuration.yml";
        String dbPass = mysql.getPassword();
        boolean createDocs = false;

        File generatedOutputFolder = new File(outputPath);
        StreamlinerUtil.deleteDirectory(generatedOutputFolder);

        updateSourceDetail(config);

        // Schema comnmand
        SchemaCommand.build(config,outputPath,dbPass,createDocs, null, null);

        Configuration outputConfig = StreamlinerUtil.readYamlFile(outputPath);

        assertNotNull(outputConfig.getTables());
        assertEquals(1, outputConfig.getTables().size());
        assertNotNull(outputConfig.getTables().get(0).getPrimaryKeys());
        assertEquals(2, outputConfig.getTables().get(0).getPrimaryKeys().size());
        assertEquals("Persons", outputConfig.getTables().get(0).getSourceName());

        StreamlinerUtil.deleteDirectory(new File("src/test/output"));
    }

  private void testSchemaCommandOptionalParams(String generatedConfigFile, String[] schemaCommand1) {
    // Scala App main method
    App.main(schemaCommand1);

    File f = new File(generatedConfigFile);
    assertTrue(f.exists());
    SchemaDefiner definer = new StreamlinerConfigReader(generatedConfigFile);
    StreamlinerCatalog catalog = definer.retrieveSchema();
    assertNotNull(catalog);
    assertNotNull(catalog.getSchemas());
    assertFalse(catalog.getSchemas().isEmpty());

    List<Schema> schemaList = (List<Schema>) catalog.getSchemas();
    schemaList.stream()
        .forEach(
            schema -> {
              if (schema.getCatalogName().equals(STREAMLINER_DATABASE_NAME)) {
                Table table = ((List<Table>) catalog.getTables(schema)).get(0);
                assertFalse(catalog.getTables(schema).isEmpty());
                assertEquals(table.getColumns().size(), 5);
                assertTrue(table.getName().equals("Persons"));
                assertEquals(STREAMLINER_DATABASE_NAME, table.getSchema().getCatalogName());
              }
            });
  }

  private void deserialize_serialize(String inputConfig) throws IOException {
    String outputPath = "src/test/output/";
    String outputConfig1 = "src/test/output/temp-config1.yml";
    String outputConfig2 = "src/test/output/temp-config2.yml";

    StreamlinerUtil.deleteDirectory(new File(outputPath));
    // deserialize
    Configuration config = StreamlinerUtil.readConfigFromPath(inputConfig);
    StreamlinerUtil.createDir(outputPath);
    // serialize
    StreamlinerUtil.writeYamlFile(config, outputConfig1);
    // deserialzie
    Configuration tempConfig = StreamlinerUtil.readConfigFromPath(outputConfig1);
    assertTrue(config.equals(tempConfig));

    // serialize
    StreamlinerUtil.writeYamlFile(tempConfig, outputConfig2);
    // deserialzie
    Configuration tempConfig2 = StreamlinerUtil.readConfigFromPath(outputConfig2);
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
