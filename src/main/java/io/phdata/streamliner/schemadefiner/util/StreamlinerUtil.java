package io.phdata.streamliner.schemadefiner.util;

import io.phdata.streamliner.schemadefiner.Mapper.HadoopMapper;
import io.phdata.streamliner.schemadefiner.Mapper.SnowflakeMapper;
import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.Source;
import io.phdata.streamliner.schemadefiner.model.TableDefinition;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
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

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class StreamlinerUtil {
    private static final Logger log = LoggerFactory.getLogger(StreamlinerUtil.class);

    public static Configuration readConfigFromPath(String path) throws FileNotFoundException {
        Yaml yaml = new Yaml(new Constructor(Configuration.class));
        File configFile = new File(path);
        InputStream inputStream = new FileInputStream(configFile);
        Configuration config = yaml.load(inputStream);
        return config;
    }

    public static Connection getConnection(String jdbcUrl, String userName, String password) {
        DatabaseConnectionSource dataSource = new DatabaseConnectionSource(jdbcUrl);
        dataSource.setUserCredentials(new SingleUseUserCredentials(userName, password));
        return dataSource.get();
    }

    public static Configuration mapJdbcCatalogToConfig(Configuration ingestConfig, StreamlinerCatalog catalog) {
        List<Schema> schema = catalog.getSchemas().stream()
                .filter(db -> db.getCatalogName().equals(ingestConfig.getSource().getSchema())).collect(Collectors.toList());
        List<Table> tableList = (List<Table>) catalog.getTables(schema.get(0));

        List<TableDefinition> tables = null;
        if (ingestConfig.getDestination().getType().equalsIgnoreCase("SNOWFLAKE")) {
            tables = SnowflakeMapper.mapSchemaCrawlerTables(tableList, ingestConfig.getSource().getUserDefinedTable());
        } else if (ingestConfig.getDestination().getType().equalsIgnoreCase("HADOOP")) {
            tables = HadoopMapper.mapSchemaCrawlerTables(tableList, ingestConfig.getSource().getUserDefinedTable());
        }
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

    private static void writeYamlFile(Configuration outputConfig, String outputDir) throws FileNotFoundException {
        DumperOptions opt = new DumperOptions();
        opt.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        opt.setPrettyFlow(true);

        Representer representer = new Representer() {
            @Override
            protected NodeTuple representJavaBeanProperty(Object javaBean, Property property, Object propertyValue, Tag customTag) {
                // if value of property is null, ignore it.
                if (propertyValue == null) {
                    return null;
                } else {
                    return super.representJavaBeanProperty(javaBean, property, propertyValue, customTag);
                }
            }
        };
        // to remove tags in YAML file
        representer.addClassTag(Configuration.class, Tag.MAP);

        //TODO: keep the parameter sequence preserved. like name, environment, pipeline, source, destination
        Yaml yaml = new Yaml(representer, opt);
        PrintWriter writer = new PrintWriter(outputDir);
        yaml.dump(outputConfig, writer);
    }

    private static void createDir(String outputDir) {
        File f = new File(outputDir);
        if (!f.exists()) {
            log.debug("Creating directory: {}", outputDir);
            f.mkdirs();
        }
    }

    public static Configuration mapGlueCatalogToConfig(Configuration ingestConfig, StreamlinerCatalog catalog) {
        Source source = ingestConfig.getSource();
        Schema db = catalog.getSchemas().stream().filter(schema -> schema.getCatalogName().equalsIgnoreCase(source.getDatabase())).collect(Collectors.toList()).get(0);
        List<Table> tableList = (List<Table>) catalog.getTables(db);
        List<TableDefinition> tableDefinitions = null;
        if (ingestConfig.getDestination().getType().equalsIgnoreCase("SNOWFLAKE")) {
            tableDefinitions = SnowflakeMapper.mapAWSGlueTables(tableList, source.getUserDefinedTable());
            ingestConfig.setTables(tableDefinitions);
        } else if (ingestConfig.getDestination().getType().equalsIgnoreCase("HADOOP")) {
            tableDefinitions = HadoopMapper.mapAWSGlueTables(tableList, source.getUserDefinedTable());
            ingestConfig.setTables(tableDefinitions);
        }
        return ingestConfig;
    }

    public static void getHtmlOutput(Source source, String password, String path) throws Exception {
        execute(source, password, TextOutputFormat.html, path, "schema.html");
    }

    public static void getErdOutput(Source source, String password, String path) throws Exception {
        execute(source, password, DiagramOutputFormat.png, path, "schema.png");
    }

    private static void execute(Source source, String password, OutputFormat outputFormat, String path, String fileName) throws Exception {
        createDir(path);
        OutputOptions outputOptions = OutputOptionsBuilder.newOutputOptions(outputFormat, Paths.get(path + "/" + fileName));
        SchemaCrawlerExecutable executable = new SchemaCrawlerExecutable("schema");
        executable.setSchemaCrawlerOptions(getOptions(source));
        executable.setOutputOptions(outputOptions);
        executable.setConnection(getConnection(source.getUrl(), source.getUsername(), password));
        executable.execute();
    }

    private static SchemaCrawlerOptions getOptions(Source source) {
        setLogLevel();

        SchemaCrawlerOptions options = SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions();
        LimitOptionsBuilder limitOptionsBuilder = LimitOptionsBuilder.builder();

        limitOptionsBuilder.includeSchemas(new RegularExpressionInclusionRule(".*" + source.getSchema() + ".*"))
                .tableTypes(mapTableTypes(source.getTableTypes()));
        if (source.getUserDefinedTable() != null) {
            List<String> list = source.getUserDefinedTable().stream()
                    .map(table -> ".*" + source.getSchema() + ".*\\." + table.getName()).collect(Collectors.toList());
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
