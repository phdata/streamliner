package io.phdata.streamliner.schemadefiner.ConfigBuilder;

import io.phdata.streamliner.schemadefiner.GlueCrawler;
import io.phdata.streamliner.schemadefiner.JdbcCrawler;
import io.phdata.streamliner.schemadefiner.SchemaDefiner;
import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.GlueCatalog;
import io.phdata.streamliner.schemadefiner.model.Jdbc;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import schemacrawler.crawl.StreamlinerCatalog;

public class SchemaCommand {

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

        StreamlinerUtil.writeConfigToYaml(outputConfig, outputDirectory);
    }

    private static void writeDocs(Configuration ingestConfig, String password, String outputDir) throws Exception {
        if (outputDir.equals("") || outputDir == null) {
            outputDir = outputDir + "/" + ingestConfig.getName() + "/" + ingestConfig.getEnvironment() + "/docs";
        } else {
            outputDir = outputDir + "/docs";
        }
        Jdbc jdbc = (Jdbc) ingestConfig.getSource();
        StreamlinerUtil.getErdOutput(jdbc, password, outputDir);
        StreamlinerUtil.getHtmlOutput(jdbc, password, outputDir);
    }
}
