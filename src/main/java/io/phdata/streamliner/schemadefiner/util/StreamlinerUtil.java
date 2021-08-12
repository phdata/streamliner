package io.phdata.streamliner.schemadefiner.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.phdata.streamliner.schemadefiner.mapper.HadoopMapper;
import io.phdata.streamliner.schemadefiner.mapper.SnowflakeMapper;
import io.phdata.streamliner.schemadefiner.model.*;
import org.apache.commons.text.StringSubstitutor;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class StreamlinerUtil {
    private static final Logger log = LoggerFactory.getLogger(StreamlinerUtil.class);

    public static Configuration readConfigFromPath(String path){
        if (path == null) return null;
        if(!fileExists(path)){
            throw new RuntimeException(String.format("Configuration file not found: %s", path));
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Configuration config = null;
        try {
            config = mapper.readValue(new File(path), Configuration.class);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error reading configuration file %s", path), e);
        }
        return config;
    }

    private static class EnvSubstStringDeserializer extends StdScalarDeserializer<String> {
        private final StringDeserializer delegate;

        protected EnvSubstStringDeserializer() {
            super(String.class);
            delegate = new StringDeserializer();
        }

        @Override
        public String deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            String result = delegate.deserialize(p, ctxt);
            return StringSubstitutor.replace(result, System.getenv(), "${env:", "}");
        }
    }

  public static Configuration readYamlFile(String path){
    if (path == null) return null;
    Path yamlFile = Paths.get(path);
    // this method handles even if yaml file is empty. Through normal process jackson throws exception
    // if yaml file is empty.
    YAMLMapper yamlMapper = new YAMLMapper();
    JsonNode tree;
    try {
      try (InputStream inputStream = Files.newInputStream(yamlFile)) {
        tree = yamlMapper.readTree(inputStream);
      }
      if (tree.isEmpty()) {
        return null;
      }
      ObjectMapper objectMapper = new ObjectMapper();
      SimpleModule module = new SimpleModule();
      module.addDeserializer(String.class, new EnvSubstStringDeserializer());
      objectMapper.registerModule(module);
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      return objectMapper.treeToValue(tree, Configuration.class);
    } catch (IOException e) {
        throw new RuntimeException("Error reading file: " + path, e);
    }
  }

    public static ConfigurationDiff readConfigDiffFromPath(String path){
        if (path == null) return null;
        if(!fileExists(path)){
            throw new RuntimeException(String.format("Configuration difference file not found: %s", path));
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        ConfigurationDiff config = null;
        try {
            config = mapper.readValue(new File(path), ConfigurationDiff.class);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error reading configuration diff file: %s", path), e);
        }
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
        if (schema.isEmpty()) {
            throw new IllegalStateException(String.format("No result found for %s", jdbc.getSchema()));
        }
        List<Table> tableList = (List<Table>) catalog.getTables(schema.get(0));
        if(tableList == null || tableList.isEmpty()){
            throw new RuntimeException(String.format("Schema: %s, does not exist in source system",jdbc.getSchema()));
        }
        List<TableDefinition> tables = null;
        if (ingestConfig.getDestination() instanceof Snowflake) {
            tables = SnowflakeMapper.mapSchemaCrawlerTables(tableList, jdbc.getUserDefinedTable());
        } else if (ingestConfig.getDestination() instanceof Hadoop) {
            tables = HadoopMapper.mapSchemaCrawlerTables(tableList, jdbc.getUserDefinedTable());
        } else {
            throw new RuntimeException(String.format("Unknown Destination provided: %s", ingestConfig.getDestination().getType()));
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

    public static void writeConfigToYaml(Configuration outputConfig, String outputDir, String fileName){
        createDir(outputDir);
        writeYamlFile(outputConfig, fileName);
    }

    public static void writeConfigToYaml(ConfigurationDiff outputConfig, String outputFile){
        createDir(getOutputDirectory(outputFile));
        writeYamlFile(outputConfig, outputFile);
    }

  public static void writeYamlFile(Configuration outputConfig, String outputFile) {
    ObjectMapper mapper =
        new ObjectMapper(
            new YAMLFactory()
                .disable(Feature.WRITE_DOC_START_MARKER)
                .disable(Feature.USE_NATIVE_TYPE_ID));
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    try {
      mapper.writeValue(new File(outputFile), outputConfig);
    } catch (IOException e) {
      throw new RuntimeException("Error writing file: " + outputFile, e);
    }
  }

  public static void writeYamlFile(ConfigurationDiff outputConfig, String outputFile) {
    ObjectMapper mapper =
        new ObjectMapper(
            new YAMLFactory()
                .disable(Feature.WRITE_DOC_START_MARKER)
                .disable(Feature.USE_NATIVE_TYPE_ID));
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

    try {
      mapper.writeValue(new File(outputFile), outputConfig);
    } catch (IOException e) {
      throw new RuntimeException("Error writing file: " + outputFile, e);
    }
  }

    public static void createDir(String outputDir) {
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
        } else {
            throw new RuntimeException(String.format("Unknown Destination provided: %s", ingestConfig.getDestination().getType()));
        }
        return ingestConfig;
    }

    public static void getHtmlOutput(Jdbc jdbc, String password, String path){
        execute(jdbc, password, TextOutputFormat.html, path, "schema.html");
    }

    public static void getErdOutput(Jdbc jdbc, String password, String path){
        execute(jdbc, password, DiagramOutputFormat.png, path, "schema.png");
    }

    private static void execute(Jdbc jdbc, String password, OutputFormat outputFormat, String path, String fileName) {
        createDir(path);
        OutputOptions outputOptions = OutputOptionsBuilder.newOutputOptions(outputFormat, Paths.get(path + "/" + fileName));
        SchemaCrawlerExecutable executable = new SchemaCrawlerExecutable("schema");
        executable.setSchemaCrawlerOptions(getOptions(jdbc));
        executable.setOutputOptions(outputOptions);
        executable.setConnection(getConnection(jdbc.getUrl(), jdbc.getUsername(), password));
        try {
            executable.execute();
        } catch (Exception e) {
            log.error("Error executing schema crawler. Error: {}", e.getMessage());
            throw new RuntimeException("Error executing schema crawler.", e);
        }
    }

    public static SchemaCrawlerOptions getOptions(Jdbc jdbc) {
        setLogLevel();

        SchemaCrawlerOptions options = SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions();
        LimitOptionsBuilder limitOptionsBuilder = LimitOptionsBuilder.builder();

        limitOptionsBuilder
            .includeSchemas(
                new RegularExpressionInclusionRule(String.format(".*%s.*", jdbc.getSchema())))
            .tableTypes(mapTableTypes(jdbc.getTableTypes()));
        // table whitelisting
        if (jdbc.getTables() != null) {
            List<String> list = jdbc.getTables().stream()
                    .map(table -> String.format(".*%s.*\\.%s", jdbc.getSchema(), table)).collect(Collectors.toList());
            String tableList = String.join("|", list);
            limitOptionsBuilder.includeTables(new RegularExpressionInclusionRule(String.format("(%s)",tableList)));
        }
        return options.withLimitOptions(limitOptionsBuilder.toOptions());
    }

    public static List<String> mapTableTypes(List<String> tableTypes) {
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

  public static boolean deleteDirectory(File directoryToBeDeleted) {
    log.debug("Deleting directory: {}", directoryToBeDeleted.getPath());
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  public static boolean fileExists(String configPath) {
    return Files.exists(Paths.get(configPath));
  }

  public static Map<String, Map<String, String>> readTypeMappingFile(String path) {
    if (!fileExists(path)) {
      throw new RuntimeException(String.format("Type mapping file not found: %s", path));
    }
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    Map<String, Map<String, String>> typeMappingFile = null;
    try {
      typeMappingFile = mapper.readValue(new File(path), Map.class);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error reading Type mapping file: %s", path), e);
    }
    return typeMappingFile;
  }

  public static List<File> listFilesInDir(String path){
    File d = new File(path);
    if (d.exists() && d.isDirectory()) {
      return Arrays.stream(d.listFiles())
          .filter(file -> file.isFile())
          .collect(Collectors.toList());
    } else {
      throw new RuntimeException(String.format("Directory path does not exist: %s", path));
    }
  }

  public static void writeFile(String content, String fileName) {
    log.info("Writing file: {}", fileName);
    log.trace("File content: {}", content);

    try (FileWriter fw = new FileWriter(fileName)) {
      fw.write(content);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error writing file: %s", fileName), e);
    }
  }

  public static void isExecutable(String path) {
    File file = new File(path);
    log.debug("File: {}, executable flag: {}", path, file.canExecute());
    if (file.canExecute()) {
      setExecutable(path);
    }
  }

  private static void setExecutable(String path) {
    File f = new File(path);
    f.setExecutable(true);
  }

  public static String readFile(String path) {
    log.debug("Reading file: {}", path);
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new FileReader(path))) {
      while (br.ready()) {
        sb.append(br.readLine());
        sb.append("\n");
      }
    } catch (FileNotFoundException e) {
      log.error("File not found : {}", path);
      throw new RuntimeException(e);
    } catch (IOException e) {
      log.error("Error reading file: {}, error: {}", path, e.getMessage());
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  private static final String IDENTIFIER_WITHOUT_SPECIAL_CHARS = "^[A-Za-z0-9_]+$";

  public static String quoteIdentifierIfNeeded(String identifier) {
    String str;
    if (identifier.matches(IDENTIFIER_WITHOUT_SPECIAL_CHARS)) {
      str = identifier;
    } else {
      str = "\"" + identifier + "\"";
    }
    return str;
  }



    public static Seq<?> convertJavaListToScalaSeq(List<?> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public Seq<ColumnDefinition> convertListToSeq(List<ColumnDefinition> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

  public Seq<TableDiff> convertTableDefinitionListToSeq(List<TableDiff> inputList) {
    if (inputList == null) {
      return null;
    }
    return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
  }

    public static String getOutputDirectory(String outputFile) {
        int lastSlashIndex = outputFile.lastIndexOf("/");
        String outputDir = outputFile.substring(0, lastSlashIndex);
        return outputDir;
    }

  public static void createFile(String... files) {
    Arrays.asList(files).stream()
        .forEach(
            file -> {
              String dir = getOutputDirectory(file);
              createDir(dir);
              File f = new File(file);
              try {
                f.createNewFile();
              } catch (IOException e) {
                throw new RuntimeException(String.format("Error creating file: %s", file));
              }
            });
  }

  public static final Set<String> snowflakeStringDataType =
      new HashSet<>(
          Arrays.asList(
              new String[] {
                "VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT", "BINARY", "VARBINARY"
              }));
}
