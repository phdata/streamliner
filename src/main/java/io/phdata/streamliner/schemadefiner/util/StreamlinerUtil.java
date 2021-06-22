package io.phdata.streamliner.schemadefiner.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import io.phdata.streamliner.schemadefiner.Mapper.HadoopMapper;
import io.phdata.streamliner.schemadefiner.Mapper.SnowflakeMapper;
import io.phdata.streamliner.schemadefiner.model.*;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.crawl.StreamlinerCatalog;
import schemacrawler.inclusionrule.RegularExpressionInclusionRule;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.LimitOptionsBuilder;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder;
import schemacrawler.tools.command.text.diagram.options.DiagramOutputFormat;
import schemacrawler.tools.command.text.schema.options.TextOutputFormat;
import schemacrawler.tools.databaseconnector.DatabaseConnectionSource;
import schemacrawler.tools.databaseconnector.SingleUseUserCredentials;
import schemacrawler.tools.executable.SchemaCrawlerExecutable;
import schemacrawler.tools.options.OutputFormat;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.options.OutputOptionsBuilder;
import us.fatehi.utility.LoggingConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class StreamlinerUtil {
    private static final Logger log = LoggerFactory.getLogger(StreamlinerUtil.class);

    public static Configuration readConfigFromPath(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Configuration config = mapper.readValue(new File(path), Configuration.class);
        return config;
    }

    public static ConfigurationDiff readConfigDiffFromPath(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        ConfigurationDiff config = mapper.readValue(new File(path), ConfigurationDiff.class);
        return config;
    }

    public static Connection getConnection(String jdbcUrl, String userName, String password) {
        DatabaseConnectionSource dataSource = new DatabaseConnectionSource(jdbcUrl);
        dataSource.setUserCredentials(new SingleUseUserCredentials(userName, password));
        return dataSource.get();
    }

    public static Configuration mapJdbcCatalogToConfig(Configuration ingestConfig, StreamlinerCatalog catalog) {
        Jdbc jdbc = (Jdbc) ingestConfig.getSource();
        List<Schema> schema = catalog.getSchemas().stream()
                .filter(db -> db.getCatalogName().equals(jdbc.getSchema())).collect(Collectors.toList());
        List<Table> tableList = (List<Table>) catalog.getTables(schema.get(0));

        List<TableDefinition> tables = null;
        if (ingestConfig.getDestination() instanceof Snowflake) {
            tables = SnowflakeMapper.mapSchemaCrawlerTables(tableList, jdbc.getUserDefinedTable());
        } else if (ingestConfig.getDestination() instanceof Hadoop) {
            tables = HadoopMapper.mapSchemaCrawlerTables(tableList, jdbc.getUserDefinedTable());
        }
        jdbc.setDriverClass(catalog.getDriverClassName());
        Configuration newConfig = new Configuration(
                ingestConfig.getName(),
                ingestConfig.getEnvironment(),
                ingestConfig.getPipeline(),
                ingestConfig.getSource(),
                ingestConfig.getDestination(),
                tables);
        return newConfig;
    }

    public static void writeConfigToYaml(Configuration outputConfig, String outputDir) throws IOException {
        if (outputDir.equals("") || outputDir == null) {
            outputDir = outputDir + "/" + outputConfig.getName() + "/" + outputConfig.getEnvironment() + "/conf";
        } else {
            outputDir = outputDir + "/conf";
        }
        createDir(outputDir);
        outputDir = outputDir + "/streamliner-configuration.yml";
        writeYamlFile(outputConfig, outputDir);

    }

    public static void writeConfigToYaml(ConfigurationDiff outputConfig, String outputDir) throws IOException {
        if (outputDir.equals("") || outputDir == null) {
            outputDir = outputDir + "/" + outputConfig.getName() + "/" + outputConfig.getEnvironment() + "/confDiff";
        } else {
            outputDir = outputDir + "/confDiff";
        }
        createDir(outputDir);
        outputDir = outputDir + "/streamliner-configuration-diff.yml";
        writeYamlFile(outputConfig, outputDir);

    }

  public static void writeYamlFile(Configuration outputConfig, String outputDir)
      throws IOException {
    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER).disable(Feature.USE_NATIVE_TYPE_ID));
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.writeValue(new File(outputDir), outputConfig);
  }

    public static void writeYamlFile(ConfigurationDiff outputConfig, String outputDir)
            throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER).disable(Feature.USE_NATIVE_TYPE_ID));
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        mapper.writeValue(new File(outputDir), outputConfig);
    }

    private static void createDir(String outputDir) {
        File f = new File(outputDir);
        if (!f.exists()) {
            log.debug("Creating directory: {}", outputDir);
            f.mkdirs();
        }
    }

    public static Configuration mapGlueCatalogToConfig(Configuration ingestConfig, StreamlinerCatalog catalog) {
        GlueCatalog source = (GlueCatalog) ingestConfig.getSource();
        Schema db = catalog.getSchemas().stream().filter(schema -> schema.getCatalogName().equalsIgnoreCase(source.getDatabase())).collect(Collectors.toList()).get(0);
        List<Table> tableList = (List<Table>) catalog.getTables(db);
        List<TableDefinition> tableDefinitions = null;
        if (ingestConfig.getDestination() instanceof Snowflake) {
            tableDefinitions = SnowflakeMapper.mapAWSGlueTables(tableList, source.getUserDefinedTable());
            ingestConfig.setTables(tableDefinitions);
        } else if (ingestConfig.getDestination() instanceof Hadoop) {
            tableDefinitions = HadoopMapper.mapAWSGlueTables(tableList, source.getUserDefinedTable());
            ingestConfig.setTables(tableDefinitions);
        }
        return ingestConfig;
    }

    public static void getHtmlOutput(Jdbc jdbc, String password, String path) throws Exception {
        execute(jdbc, password, TextOutputFormat.html, path, "schema.html");
    }

    public static void getErdOutput(Jdbc jdbc, String password, String path) throws Exception {
        execute(jdbc, password, DiagramOutputFormat.png, path, "schema.png");
    }

    private static void execute(Jdbc jdbc, String password, OutputFormat outputFormat, String path, String fileName) throws Exception {
        createDir(path);
        OutputOptions outputOptions = OutputOptionsBuilder.newOutputOptions(outputFormat, Paths.get(path + "/" + fileName));
        SchemaCrawlerExecutable executable = new SchemaCrawlerExecutable("schema");
        executable.setSchemaCrawlerOptions(getOptions(jdbc));
        executable.setOutputOptions(outputOptions);
        executable.setConnection(getConnection(jdbc.getUrl(), jdbc.getUsername(), password));
        executable.execute();
    }

    private static SchemaCrawlerOptions getOptions(Jdbc jdbc) {
        setLogLevel();

        SchemaCrawlerOptions options = SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions();
        LimitOptionsBuilder limitOptionsBuilder = LimitOptionsBuilder.builder();

        limitOptionsBuilder.includeSchemas(new RegularExpressionInclusionRule(".*" + jdbc.getSchema() + ".*"))
                .tableTypes(mapTableTypes(jdbc.getTableTypes()));
        if (jdbc.getUserDefinedTable() != null) {
            List<String> list = jdbc.getUserDefinedTable().stream()
                    .map(table -> ".*" + jdbc.getSchema() + ".*\\." + table.getName()).collect(Collectors.toList());
            String tableList = String.join("|", list);
            limitOptionsBuilder.includeTables(new RegularExpressionInclusionRule(tableList));
        }
        return options.withLimitOptions(limitOptionsBuilder.toOptions());
    }

    private static List<String> mapTableTypes(List<String> tableTypes) {
        return tableTypes.stream().map(tableType -> {
            if (tableType.equals("tables")) {
                return "table";
            } else if (tableType.equals("views")) {
                return "view";
            } else {
                return tableType;
            }
        }).collect(Collectors.toList());
    }

    private static void setLogLevel() {
        org.apache.log4j.Logger logManager = LogManager.getRootLogger();
        String str = logManager.getLevel().toString();
        Level level;
        if (str.equals("OFF")) {
            level = Level.OFF;
        } else if (str.equals("ERROR") || str.equals("FATAL") || str.equals("SEVERE")) {
            level = Level.SEVERE;
        } else if (str.equals("WARN") || str.equals("WARNING")) {
            level = Level.WARNING;
        } else if (str.equals("CONFIG") || str.equals("DEBUG")) {
            level = Level.CONFIG;
        } else if (str.equals("INFO")) {
            level = Level.INFO;
        } else if (str.equals("TRACE")) {
            level = Level.FINER;
        } else {
            level = Level.ALL;
        }
        new LoggingConfig(level);
    }
}
