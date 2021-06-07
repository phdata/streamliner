package schemacrawler.crawl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.phdata.streamliner.configuration.ConfigurationBuilder;
import io.phdata.streamliner.pipeline.PipelineBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.shaded.org.yaml.snakeyaml.Yaml;
import org.testcontainers.utility.DockerImageName;
import scala.Option;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class MySQLTestContainerTest {

    private static final DockerImageName MYSQL_57_IMAGE = DockerImageName.parse("mysql:5.7.34");
    private Connection con = null;
    private Yaml yaml = new Yaml();

    @Rule
    public MySQLContainer mysql = new MySQLContainer(MYSQL_57_IMAGE)
            .withDatabaseName("STREAMLINER_DB")
            .withUsername("streamliner_user")
            .withPassword("streamliner_pwd");

    @Before
    public void before() throws SQLException {
        mysql.start();
        DataSource ds = getDataSource(mysql);
        con = ds.getConnection();
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
    public void testStreamlinerSchemaCommand() throws FileNotFoundException {
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
    public void testStreamlinerScriptCommand() {
        String config = "src/test/output/conf/streamliner-configuration.yml";
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

    private DataSource getDataSource(JdbcDatabaseContainer<?> container) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        return new HikariDataSource(hikariConfig);
    }
}
