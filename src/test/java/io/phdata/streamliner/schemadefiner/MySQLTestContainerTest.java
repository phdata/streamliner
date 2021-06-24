package io.phdata.streamliner.schemadefiner;

import io.phdata.streamliner.App;
import io.phdata.streamliner.configuration.ConfigurationBuilder;
import io.phdata.streamliner.pipeline.PipelineBuilder;
import io.phdata.streamliner.schemadefiner.ConfigBuilder.SchemaCommand;
import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.shaded.org.yaml.snakeyaml.Yaml;
import org.testcontainers.utility.DockerImageName;
import scala.Option;
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
                "    PersonID int,\n" +
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
    public void test1StreamlinerSchemaCommand() throws FileNotFoundException {
        String config = "src/test/resources/conf/ingest-configuration.yml";
        String outputPath = "src/test/output/";
        String dbPass = mysql.getPassword();
        boolean createDocs = false;
        Map<String, Object> ingestConfigFile = updateSourceDetail(config);
        File generatedOutputFolder = new File(outputPath);
        deleteDirectory(generatedOutputFolder);

        //streamliner schema command
        ConfigurationBuilder.build(config, Option.apply(outputPath), Option.apply(dbPass), createDocs);

        generatedOutputFolder = new File(outputPath + "conf");
        assertTrue(generatedOutputFolder.exists());
        assertTrue(generatedOutputFolder.isDirectory());

        File outputFile = new File(outputPath + "conf/streamliner-configuration.yml");
        assertTrue(outputFile.exists());
        assertTrue(outputFile.isFile());

        InputStream inputStream = new FileInputStream(outputFile);
        Map<String, Object> outputConfigFile = yaml.load(inputStream);

        assertEquals(ingestConfigFile.get("name"), outputConfigFile.get("name"));
        assertEquals(ingestConfigFile.get("pipeline"), outputConfigFile.get("pipeline"));

        assertEquals(((Map<String, Object>) ingestConfigFile.get("source")).get("schema"), ((Map<String, Object>) outputConfigFile.get("source")).get("schema"));
        assertEquals(((Map<String, Object>) ingestConfigFile.get("destination")).get("type"), ((Map<String, Object>) outputConfigFile.get("destination")).get("type"));

        Map<String, Object> table = ((List<Map<String, Object>>) outputConfigFile.get("tables")).get(0);
        assertEquals(table.get("type"), "Snowflake");
        assertEquals(table.get("sourceName"), "Persons");

        List<Map<String, Object>> columnList = ((List<Map<String, Object>>) table.get("columns"));
        assertFalse(columnList.isEmpty());
    }

    @Test
    public void test2StreamlinerScriptCommand() {
        String config = SCHEMA_COMMAND_OUTPUT_PATH;
        String templateDirectory = "src/main/resources/templates/snowflake";
        String typeMapping = "src/main/resources/type-mapping.yml";
        String outputPath = "src/test/output/pipelineConfig";

        File generatedOutputFolder = new File(outputPath);
        deleteDirectory(generatedOutputFolder);

        //Streamliner script command
        PipelineBuilder.build(config, typeMapping, templateDirectory, Option.apply(outputPath));

        assertTrue(generatedOutputFolder.exists());
        String[] allContents = generatedOutputFolder.list();

        //Person directory and Makefile
        assertEquals(allContents.length, 2);

        File generatedPersonsDir = new File("src/test/output/pipelineConfig/Persons");
        assertTrue(generatedPersonsDir.exists());

        allContents = generatedPersonsDir.list();
        // 10 files are generated. copy-into.sql, create-schema.sql, create-snowpipe.sql, create-stage.sql, create-table.sql,
        // drop-schema.sql, drop-snowpipe.sql, drop-stage.sql,  drop-table.sql, Makefile
        assertEquals(allContents.length, 10);
    }

    @Test
    public void test3JdbcCrawler() throws Exception {
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
                assertEquals(table.getColumns().size(),  5);
                assertTrue(table.getName().equals("Persons"));
                assertEquals(table.getSchema().getCatalogName(),STREAMLINER_DATABASE_NAME);
            }
        });

        assertNotNull(catalog.getDriverClassName());
        assertEquals(mysql.getDriverClassName(), catalog.getDriverClassName());
    }

    @Test
    public void test4StreamlinerConfigReader() throws Exception {
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
                assertEquals(table.getColumns().size(),  5);
                assertTrue(table.getName().equals("Persons"));
                assertEquals(table.getSchema().getCatalogName(),STREAMLINER_DATABASE_NAME);
            }
        });
        assertNotNull(catalog.getDriverClassName());
        assertEquals(mysql.getDriverClassName(), catalog.getDriverClassName());
    }

    @Test
    public void test5SchemaCommand_new() throws Exception {
        String config = "src/test/resources/conf/ingest-configuration.yml";
        String outputPath = "src/test/output/";
        String dbPass = mysql.getPassword();
        boolean createDocs = false;

        File generatedOutputFolder = new File(outputPath);
        deleteDirectory(generatedOutputFolder);

        Map<String, Object> ingestConfigFile = updateSourceDetail(config);

        // schema command with new implementations
        SchemaCommand.build(config,outputPath,dbPass,createDocs);

        generatedOutputFolder = new File(outputPath + "conf");
        assertTrue(generatedOutputFolder.exists());
        assertTrue(generatedOutputFolder.isDirectory());

        File outputFile = new File(outputPath + "conf/streamliner-configuration.yml");
        assertTrue(outputFile.exists());
        assertTrue(outputFile.isFile());

        InputStream inputStream = new FileInputStream(outputFile);
        Map<String, Object> outputConfigFile = yaml.load(inputStream);

        assertEquals(ingestConfigFile.get("name"), outputConfigFile.get("name"));
        assertEquals(ingestConfigFile.get("pipeline"), outputConfigFile.get("pipeline"));

        assertEquals(((Map<String, Object>) ingestConfigFile.get("source")).get("schema"), ((Map<String, Object>) outputConfigFile.get("source")).get("schema"));
        assertEquals(((Map<String, Object>) ingestConfigFile.get("destination")).get("type"), ((Map<String, Object>) outputConfigFile.get("destination")).get("type"));

        Map<String, Object> table = ((List<Map<String, Object>>) outputConfigFile.get("tables")).get(0);
        assertEquals(table.get("type"), "Snowflake");
        assertEquals(table.get("sourceName"), "Persons");

        List<Map<String, Object>> columnList = ((List<Map<String, Object>>) table.get("columns"));
        assertFalse(columnList.isEmpty());
    }

    //@Test
    public void testGlueCrawler_WIP() throws Exception {
        String config = "src/test/resources/conf/ingest-configuration-glue.yml";
        String outputPath = "src/test/output/";
        String dbPass = mysql.getPassword();
        boolean createDocs = false;

        File generatedOutputFolder = new File(outputPath);
        deleteDirectory(generatedOutputFolder);
        SchemaCommand.build(config,outputPath,dbPass,createDocs);
    }

  @Test
  public void test6ConfigurationDiff_serialize() throws IOException {
    String outputPath = "src/test/output/";
    String configPath1 = "src/test/resources/conf/ingest-configuration.yml";
    String configPath2 = "src/test/resources/conf/ingest-configuration-glue.yml";

    // reading previous destination config
    Configuration conf1 = StreamlinerUtil.readConfigFromPath(configPath1);
      // reading current destination config
    Configuration conf2 = StreamlinerUtil.readConfigFromPath(configPath2);

    ColumnDefinition prevColDef1 = new ColumnDefinition("ID", "ID", "Number", "Emp Id", 255, 0);
    ColumnDefinition currColDef1 = new ColumnDefinition("Emp_Id", "Emp_Id", "Number", "Employee Id", 255, 0);
    ColumnDiff columnDiff1 = new ColumnDiff(prevColDef1, currColDef1, false, false, true);

    ColumnDefinition prevColDef2 =
        new ColumnDefinition("Name", "Name", "Varchar2", "Emp Name", 255, 0);
    ColumnDefinition currColDef2 =
        new ColumnDefinition("Emp_Name", "Emp_Name", "Varchar2", "Employee Name", 255, 0);
    ColumnDiff columnDiff2 = new ColumnDiff(prevColDef2, currColDef2, false, false, true);

    List<ColumnDiff> colList = new ArrayList<>();
    colList.add(columnDiff1);
    colList.add(columnDiff2);
    TableDiff tableDiff1 = new TableDiff("Snowflake", "Employee", colList, true);

    colList = new ArrayList<>();
    colList.add(columnDiff1);
    TableDiff tableDiff2 = new TableDiff("Snowflake", "Emp", colList, true);

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

    deleteDirectory(new File(outputPath));
    StreamlinerUtil.writeConfigToYaml(configDiff, outputPath);
  }

  @Test
  public void test7ConfigurationDiff_deserialize() throws IOException{
    String configDiffPath = "src/test/output/confDiff/streamliner-configuration-diff.yml";
    // reading config diff yaml file.
    ConfigurationDiff configDiff = StreamlinerUtil.readConfigDiffFromPath(configDiffPath);

    assertEquals(configDiff.getName(), "STREAMLINER_QUICKSTART_1");
    assertEquals(configDiff.getEnvironment(), "SANDBOX");
    assertEquals(configDiff.getPipeline(), "snowflake-snowpipe-append");

    assertNotNull(configDiff.getPreviousDestination());
    Snowflake prevDestination = (Snowflake) configDiff.getPreviousDestination();
    Snowflake currDestination = (Snowflake) configDiff.getCurrentDestination();
    assertEquals(prevDestination.getSnowSqlCommand(), "snowsql -c connection");
    assertEquals(prevDestination.getStoragePath(), "s3://streamliner-quickstart-1/employees/");

    assertNotNull(configDiff.getCurrentDestination());
    assertEquals(currDestination.getSnowSqlCommand(), "snowsql -c streamliner_admin");
    assertEquals(
        currDestination.getStoragePath(), "s3://phdata-snowflake-stage/data/phdata-task/HR/");

    assertNotNull(configDiff.getTableDiffs());
    assertFalse(configDiff.getTableDiffs().isEmpty());
    assertEquals(configDiff.getTableDiffs().size(), 2);

    configDiff.getTableDiffs().stream()
        .forEach(
            tableDiff -> {
              if (tableDiff.getDestinationName().equals("Employee")) {
                assertEquals(tableDiff.getColumnDiffs().size(), 2);
              } else if (tableDiff.getDestinationName().equals("Emp")) {
                assertEquals(tableDiff.getColumnDiffs().size(), 1);
              }
            });
  }

  @Test
  public void test8_deserialize_serialize() throws IOException {
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
  public void test9ScalaAppMainMethod_usingNewSchemaCommand_() throws Exception {
    String config = "src/test/resources/conf/ingest-configuration.yml";
    String generatedConfigFile = "src/test/output/DEV_SDW/conf/streamliner-configuration.yml";
    updateSourceDetail(config);

    // --create-docs is optional
    String schemaCommand1[] = {
      "schema",
      "--config",
      config,
      "--output-path",
      "src/test/output/DEV_SDW/",
      "--database-password",
      mysql.getPassword()
    };
    testSchemaCommandOptionalParams(generatedConfigFile, schemaCommand1);

    generatedConfigFile = "STREAMLINER_QUICKSTART_1/SANDBOX/conf/streamliner-configuration.yml";
    // --output-path is optional
    String schemaCommand2[] = {
      "schema", "--config", config, "--database-password", mysql.getPassword()
    };
    testSchemaCommandOptionalParams(generatedConfigFile, schemaCommand2);
    deleteDirectory(new File("STREAMLINER_QUICKSTART_1"));
  }

  @Test
  public void test10SchemaCommandOptionalPassword() {
    /* --database-password is optional parameter. But if source type is Jdbc then password is mandatory */
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("A databasePassword is required when crawling JDBC source");
    String config = "src/test/resources/conf/ingest-configuration.yml";
    // --database-password is optional
    String schemaCommand[] = {"schema", "--config", config};
    // Scala App main method
    App.main(schemaCommand);
  }

  private void testSchemaCommandOptionalParams(String generatedConfigFile, String[] schemaCommand1)
      throws Exception {
    String outputPath = "src/test/output/";
    deleteDirectory(new File(outputPath));

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
                assertEquals(table.getSchema().getCatalogName(), STREAMLINER_DATABASE_NAME);
              }
            });
  }

  private void deserialize_serialize(String inputConfig) throws IOException {
    String outputPath = "src/test/output/";
    String outputConfig1 = "src/test/output/temp-config1.yml";
    String outputConfig2 = "src/test/output/temp-config2.yml";

    deleteDirectory(new File(outputPath));
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

    boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
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
}
