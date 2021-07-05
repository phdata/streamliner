package io.phdata.streamliner.schemadefiner.ConfigBuilder;

import io.phdata.streamliner.schemadefiner.GlueCrawler;
import io.phdata.streamliner.schemadefiner.JdbcCrawler;
import io.phdata.streamliner.schemadefiner.SchemaDefiner;
import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.GlueCatalog;
import io.phdata.streamliner.schemadefiner.model.Jdbc;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.crawl.StreamlinerCatalog;

public class SchemaCommand {
    private static final Logger log = LoggerFactory.getLogger(SchemaCommand.class);

    public static void build(String configurationFile, String outputFile, String password, boolean createDocs, String previousOutputFile, String diffOutputFile) throws Exception {
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
        throw new RuntimeException("Either previous-output-file or diff-output-file is not provided");
    }
        log.info("Starting Schema Crawling......");
        // read ingest-configuration.yml
        Configuration ingestConfig = StreamlinerUtil.readConfigFromPath(configurationFile);
        Configuration outputConfig = null;
        if(outputFile.equals("") || !isConfigFile(outputFile)){
            log.error("output-file should be a .yml/.yaml file.");
            throw new RuntimeException("output-file should be a .yml/.yaml file.");
        }
        if (ingestConfig.getSource() instanceof Jdbc) {
            if (password == null || password.equals("")) {
                log.error("A databasePassword is required when crawling JDBC sources");
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
        }

        StreamlinerUtil.writeConfigToYaml(outputConfig, StreamlinerUtil.getOutputDirectory(outputFile) ,outputFile);
        log.info("Schema crawl is successful and configuration file is written to : {}", outputFile);

    if (previousOutputFile != null && diffOutputFile != null) {
      if (!isConfigFile(diffOutputFile)) {
          log.error("diff-output-file should be a .yml/.yaml file");
        throw new RuntimeException("diff-output-file should be a .yml/.yaml file");
      }
      if (!isConfigFile(previousOutputFile)) {
          log.error("previous-output-file should be a .yml/.yaml file");
        throw new RuntimeException("previous-output-file should be a .yml/.yaml file");
      }
      log.info("Calculating configuration differences....");
      log.debug(
          "previous-output-file: {},  current config: {}, diff-output-file: {} ",
          previousOutputFile,
          outputFile,
          diffOutputFile);
      SchemaEvolution.build(previousOutputFile, outputFile, diffOutputFile);
    }
    }

    private static void writeDocs(Configuration ingestConfig, String password, String outputFile) throws Exception {
        // finding the directory
        String outputDir = StreamlinerUtil.getOutputDirectory(outputFile);
        outputDir =  String.format("%s/docs", outputDir);

        Jdbc jdbc = (Jdbc) ingestConfig.getSource();
        StreamlinerUtil.getErdOutput(jdbc, password, outputDir);
        StreamlinerUtil.getHtmlOutput(jdbc, password, outputDir);
    }

    private static boolean isConfigFile(String file){
        return file.endsWith(".yml") || file.endsWith(".yaml");
    }
}
