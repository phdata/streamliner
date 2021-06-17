package io.phdata.streamliner.schemadefiner;

import io.phdata.streamliner.configuration.ConfigurationBuilder;
import io.phdata.streamliner.pipeline.PipelineBuilder;
import io.phdata.streamliner.schemadefiner.ConfigBuilder.SchemaCommand;
import io.phdata.streamliner.schemadefiner.model.ColumnDiff;
import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.ConfigurationDiff;
import io.phdata.streamliner.schemadefiner.model.TableDiff;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.junit.*;
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
    private Connection con = null;
    private Yaml yaml = new Yaml();

    @Rule
    public MySQLContainer mysql = new MySQLContainer(MYSQL_57_IMAGE)
            .withDatabaseName(STREAMLINER_DATABASE_NAME)
            .withUsername(STREAMLINER_DATABASE_USERNAME)
            .withPassword(STREAMLINER_DATABASE_PASSWORD);

    @Before
    public void before() throws SQLException {
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

    @After
    public void after() {
        mysql.stop();
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
        SchemaDefiner definer = new JdbcCrawler(mysql.getJdbcUrl(), () -> con,null,null,null);

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

    ColumnDiff columnDiff1 =
        new ColumnDiff(
            "Id",
            "Emp_Id",
            "Number",
            "Number",
            "Emp Id",
            "Employee Id",
            255,
            200,
            0,
            0,
            false,
            false,
            true);
    ColumnDiff columnDiff2 =
        new ColumnDiff(
            "Name",
            "Emp_Name",
            "Varchar",
            "Varchar2",
            "Emp Name",
            "Employee Name",
            255,
            200,
            0,
            0,
            false,
            false,
            true);
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
    assertEquals(configDiff.getPreviousDestination().getSnowSqlCommand(), "snowsql -c connection");
    assertEquals(
        configDiff.getPreviousDestination().getStoragePath(),
        "s3://streamliner-quickstart-1/employees/");

    assertNotNull(configDiff.getCurrentDestination());
    assertEquals(
        configDiff.getCurrentDestination().getSnowSqlCommand(), "snowsql -c streamliner_admin");
    assertEquals(
        configDiff.getCurrentDestination().getStoragePath(),
        "s3://phdata-snowflake-stage/data/phdata-task/HR/");

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

    private int performExecuteUpdate(Connection con, String sql) throws SQLException {
        Statement statement = con.createStatement();
        int row = statement.executeUpdate(sql);
        return row;
    }
}
