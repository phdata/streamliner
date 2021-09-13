package io.phdata.streamliner.schemadefiner.configbuilder;

import io.phdata.streamliner.schemadefiner.GlueCrawler;
import io.phdata.streamliner.schemadefiner.JdbcCrawler;
import io.phdata.streamliner.schemadefiner.SchemaDefiner;
import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.crawl.StreamlinerCatalog;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SchemaCommand {
    private static final Logger log = LoggerFactory.getLogger(SchemaCommand.class);
    // --config is ingest-configuration.yml
    public static void build(String config, String stateDirectory, String password, boolean createDocs, String previousStateDirectory){
        log.debug(
                "--config: {}, --state-directory: {}, --previous-state-directory: {}, create-docs: {}",
                config,
                stateDirectory,
                previousStateDirectory,
                createDocs);

        Boolean generateConfigDiff = false;
    // first checking all the required conditions. Otherwise after executing half of the process if
    // later it throws some error due to improper params then again it will consume time
        validateStateDirectory(stateDirectory);
        if (previousStateDirectory != null) {
            validatePreviousStateDirectory(previousStateDirectory);
          generateConfigDiff = true;
        }
        log.info("Starting Schema Crawling......");
        // read ingest-configuration.yml
        Configuration ingestConfig = StreamlinerUtil.readYamlFile(config);
        isConfigurationValid(ingestConfig);
        Configuration outputConfig = null;
        if (ingestConfig.getSource() instanceof Jdbc) {
            if (password == null || password.equals("")) {
                throw new RuntimeException("A databasePassword is required when crawling JDBC sources");
            }
            if(createDocs){
                writeDocs(ingestConfig, password, stateDirectory);
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
            throw new RuntimeException(String.format("Unknown Source provided: %s", ingestConfig.getSource().getType()));
        }
        checkConfiguration(outputConfig);
        outputConfig.getTables().forEach(table -> {
            List<TableDefinition> tableList = new ArrayList<>();
            tableList.add(table);
            Configuration conf = new Configuration(tableList);
            StreamlinerUtil.writeYamlFile(conf, String.format("%s/%s.yml", stateDirectory,table.getSourceName()));
        });

        log.info("Schema crawl is successful and configuration file per table is written to directory : {}", stateDirectory);

        if (generateConfigDiff) {
            log.info("Calculating configuration differences....");
            Configuration currentConfig = StreamlinerUtil.createConfig(stateDirectory, ingestConfig, "--state-directory");
            Configuration previousConfig = StreamlinerUtil.createConfig(previousStateDirectory, ingestConfig, "--previous-state-directory");
            log.debug("--state-directory: {}, --previous-state-directory: {}", stateDirectory, previousStateDirectory);
            SchemaEvolution.build(previousConfig, currentConfig, String.format("%s/%s", stateDirectory,Constants.STREAMLINER_DIFF_FILE.value()));
        }
    }

    private static void validatePreviousStateDirectory(String previousStateDirectory) {
    File f = new File(previousStateDirectory);
    if (f.exists()) {
      if (f.isFile()) {
        throw new RuntimeException(
            String.format(
                "Arg: %s. Expected directory. Found file: %s",
                "--previous-state-directory",
                previousStateDirectory));
      }
    } else {
      throw new RuntimeException(
          String.format("--previous-state-directory not found. Path: %s", previousStateDirectory));
    }
  }

  private static void validateStateDirectory(String stateDirectory) {
    /* --state-directory should be a directory.
    During every run existing state-directory folder is deleted and new folder is created to ensure streamliner do not stores any unwanted table config file.
    * */
    File f = new File(stateDirectory);
    if (f.exists()) {
      if (f.isFile()) {
        throw new RuntimeException(
            String.format(
                "Arg: %s. Expected directory. Found file: %s",
                "--state-directory",
                stateDirectory));
      } else {
        StreamlinerUtil.deleteDirectory(f);
        log.info("Deleted old --state-directory.");
        StreamlinerUtil.createDir(stateDirectory);
        log.info("--state-directory folder created. Path: {}", stateDirectory);
      }
    } else {
      log.info("--state-directory does not exists.");
      StreamlinerUtil.createDir(stateDirectory);
      log.info("--state-directory folder created. Path: {}", stateDirectory);
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

    private static void writeDocs(Configuration ingestConfig, String password, String stateDirectory){
        String outputDir =  String.format("%s/docs", stateDirectory);
        Jdbc jdbc = (Jdbc) ingestConfig.getSource();
        StreamlinerUtil.getErdOutput(jdbc, password, outputDir);
        StreamlinerUtil.getHtmlOutput(jdbc, password, outputDir);
    }
}
