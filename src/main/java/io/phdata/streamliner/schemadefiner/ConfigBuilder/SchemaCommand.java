package io.phdata.streamliner.schemadefiner.ConfigBuilder;

import io.phdata.streamliner.schemadefiner.GlueCrawler;
import io.phdata.streamliner.schemadefiner.JdbcCrawler;
import io.phdata.streamliner.schemadefiner.SchemaDefiner;
import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.crawl.StreamlinerCatalog;

import java.io.File;
import java.util.List;

public class SchemaCommand {
    private static final Logger log = LoggerFactory.getLogger(SchemaCommand.class);

    public static void build(String configurationFile, String outputFile, String password, boolean createDocs, String previousOutputFile, String diffOutputFile){
        log.debug(
                "config: {}, output-file: {}, previous-output-file: {}, diff-output-file: {}, create-docs: {}",
                configurationFile,
                outputFile,
                previousOutputFile,
                diffOutputFile,
                createDocs);
        if ((previousOutputFile == null && diffOutputFile != null)
                || previousOutputFile != null && diffOutputFile == null) {
            log.error("Either previous-output-file or diff-output-file is not provided.");
            throw new RuntimeException("Either previous-output-file or diff-output-file is not provided.");
        }
        Boolean generateConfigDiff = false;
    // first checking all the required conditions. Otherwise after executing half of the process if
    // later it throws some error due to improper params then again it will consume time
        isFile(outputFile, "--output-file");
        if (previousOutputFile != null && diffOutputFile != null) {
          isFile(previousOutputFile, "--previous-output-file");
          isFile(diffOutputFile, "--diff-output-file");
          generateConfigDiff = true;
        }
        log.info("Starting Schema Crawling......");
        // read ingest-configuration.yml
        Configuration ingestConfig = StreamlinerUtil.readYamlFile(configurationFile);
        isConfigurationValid(ingestConfig);
        Configuration outputConfig = null;
        if (ingestConfig.getSource() instanceof Jdbc) {
            if (password == null || password.equals("")) {
                throw new RuntimeException("A databasePassword is required when crawling JDBC sources");
            }
            if(createDocs){
                writeDocs(ingestConfig, password, outputFile);
            }
            SchemaDefiner schemaDef = new JdbcCrawler((Jdbc) ingestConfig.getSource(), password);
            StreamlinerCatalog catalog = schemaDef.retrieveSchema();
            outputConfig = StreamlinerUtil.mapJdbcCatalogToConfig(ingestConfig, catalog);
        } else if (ingestConfig.getSource() instanceof GlueCatalog) {
            GlueCatalog glue = (GlueCatalog) ingestConfig.getSource();
            SchemaDefiner schemaDef = new GlueCrawler(glue);
            StreamlinerCatalog catalog = schemaDef.retrieveSchema();
            outputConfig = StreamlinerUtil.mapGlueCatalogToConfig(ingestConfig, catalog);
        } else {
            log.error("Unknown Source provided: {}", ingestConfig.getSource().getType());
            throw new RuntimeException(String.format("Unknown Source provided: %s", ingestConfig.getSource().getType()));
        }
        checkConfiguration(outputConfig);
        StreamlinerUtil.writeConfigToYaml(outputConfig, StreamlinerUtil.getOutputDirectory(outputFile) ,outputFile);
        log.info("Schema crawl is successful and configuration file is written to : {}", outputFile);

        if (generateConfigDiff) {
            log.info("Calculating configuration differences....");
            log.debug(
                    "previous-output-file: {},  current config: {}, diff-output-file: {} ",
                    previousOutputFile,
                    outputFile,
                    diffOutputFile);
            SchemaEvolution.build(previousOutputFile, outputFile, diffOutputFile);
        }
    }

    private static void isConfigurationValid(Configuration ingestConfig) {
        if(ingestConfig == null){
            log.error("Empty configuration provided. Please provide a valid configuration file.");
            throw new RuntimeException("Empty configuration provided.");
        }
        if(ingestConfig.getSource() == null){
            log.error("Source not provided in configuration.");
            throw new RuntimeException("Source not provided in configuration.");
        }
        if(ingestConfig.getDestination() == null){
            log.error("Destination not provided in configuration.");
            throw new RuntimeException("Destination not provided in configuration.");
        }
    }

    private static void checkConfiguration(Configuration config) {
    List<TableDefinition> tables = config.getTables();
    if (tables != null) {
      tables.stream()
          .forEach(
              table -> {
                if (table instanceof HadoopTable) {
                  if (config.getPipeline().equalsIgnoreCase("INCREMENTAL-WITH-KUDU")) {
                    checkCheckColumn((HadoopTable) table);
                    checkPrimaryKeys(table);
                  } else if (config.getPipeline().equalsIgnoreCase("KUDU-TABLE-DDL")) {
                    checkPrimaryKeys(table);
                  }
                  checkNumberOfMappers((HadoopTable) table);
                } else if (table instanceof SnowflakeTable) {
                  if (config.getPipeline().equalsIgnoreCase("SNOWFLAKE-INCREMENTAL-MERGE")) {
                    checkPrimaryKeys(table);
                  }
                } else {
                    log.error("Unknown Table found: {}", table.getType());
                    throw new RuntimeException(String.format("Unknown Table found: %s", table.getType()));
                }
              });
    }
  }

  private static void checkNumberOfMappers(HadoopTable table) {
    if (table.getNumberOfMappers() > 1) {
      if (table.getSplitByColumn() == null || table.getSplitByColumn().isEmpty()) {
        throw new RuntimeException(
            String.format(
                "Table: %s, has number of mappers greater than 1 with no splitByColumn defined Sqoop import will fail for this table",
                table.getSourceName()));
      }
    }
  }

    private static void checkCheckColumn(HadoopTable table) {
    if (table.getCheckColumn() == null || table.getCheckColumn().isEmpty()) {
      log.warn("No check column is defined for table: {}", table.getSourceName());
    }
  }

  private static void checkPrimaryKeys(TableDefinition table) {
    if (table.getPrimaryKeys() == null || table.getPrimaryKeys().isEmpty()) {
      log.warn("No primary keys are defined for table: {}", table.getSourceName());
    }
  }

    private static void writeDocs(Configuration ingestConfig, String password, String outputFile){
        // finding the directory
        String outputDir = StreamlinerUtil.getOutputDirectory(outputFile);
        outputDir =  String.format("%s/docs", outputDir);

        Jdbc jdbc = (Jdbc) ingestConfig.getSource();
        StreamlinerUtil.getErdOutput(jdbc, password, outputDir);
        StreamlinerUtil.getHtmlOutput(jdbc, password, outputDir);
    }

  private static boolean isFile(String outputFile, String param) {
    File f = new File(outputFile);
    if (f.exists()) {
      if (f.isDirectory()) {
        log.error("{} is a Directory. Expecting a file: {}", param, outputFile);
        throw new RuntimeException(
            String.format("Arg: {}. Expected file. Found directory: {}", param, outputFile));
      } else {
        return true;
      }
    } else {
      log.error("{} does not exists: {}", param, outputFile);
      throw new RuntimeException(String.format("%s does not exists: %s", param, outputFile));
    }
  }
}
