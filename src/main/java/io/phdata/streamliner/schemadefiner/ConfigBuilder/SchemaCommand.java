package io.phdata.streamliner.schemadefiner.ConfigBuilder;

import io.phdata.streamliner.schemadefiner.GlueCrawler;
import io.phdata.streamliner.schemadefiner.JdbcCrawler;
import io.phdata.streamliner.schemadefiner.SchemaDefiner;
import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.crawl.StreamlinerCatalog;

import java.util.List;

public class SchemaCommand {
    private static final Logger log = LoggerFactory.getLogger(SchemaCommand.class);
    private static final String STREAMLINER_CONFIGURATION_NAME = "streamliner-configuration.yml";

    public static void build(String configurationFile, String outputDirectory, String password, boolean createDocs) throws Exception {
        // read ingest-configuration.yml
        Configuration ingestConfig = StreamlinerUtil.readConfigFromPath(configurationFile);
        Configuration outputConfig = null;
        if (ingestConfig.getSource() instanceof Jdbc) {
            if (password == null || password.equals("")) {
                throw new RuntimeException("A databasePassword is required when crawling JDBC sources");
            }
            if(createDocs){
                writeDocs(ingestConfig, password, outputDirectory);
            }
            SchemaDefiner schemaDef = new JdbcCrawler((Jdbc) ingestConfig.getSource(), password);
            StreamlinerCatalog catalog = schemaDef.retrieveSchema();
            outputConfig = StreamlinerUtil.mapJdbcCatalogToConfig(ingestConfig, catalog);
        } else if (ingestConfig.getSource() instanceof GlueCatalog) {
            GlueCatalog glue = (GlueCatalog) ingestConfig.getSource();
            SchemaDefiner schemaDef = new GlueCrawler(glue);
            StreamlinerCatalog catalog = schemaDef.retrieveSchema();
            outputConfig = StreamlinerUtil.mapGlueCatalogToConfig(ingestConfig, catalog);
        }
        checkConfiguration(outputConfig);
        StreamlinerUtil.writeConfigToYaml(outputConfig, outputDirectory, STREAMLINER_CONFIGURATION_NAME);
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

    private static void writeDocs(Configuration ingestConfig, String password, String outputDir) throws Exception {
        if (outputDir == null || outputDir.equals("")) {
            outputDir = outputDir + "/" + ingestConfig.getName() + "/" + ingestConfig.getEnvironment() + "/docs";
        } else {
            outputDir = outputDir + "/docs";
        }
        Jdbc jdbc = (Jdbc) ingestConfig.getSource();
        StreamlinerUtil.getErdOutput(jdbc, password, outputDir);
        StreamlinerUtil.getHtmlOutput(jdbc, password, outputDir);
    }
}
