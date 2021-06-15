package io.phdata.streamliner.schemadefiner.ConfigBuilder;

import io.phdata.streamliner.schemadefiner.GlueCrawler;
import io.phdata.streamliner.schemadefiner.JdbcCrawler;
import io.phdata.streamliner.schemadefiner.SchemaDefiner;
import io.phdata.streamliner.schemadefiner.model.Configuration;
import io.phdata.streamliner.schemadefiner.model.Source;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import schemacrawler.crawl.StreamlinerCatalog;

import java.sql.Connection;
import java.util.List;

public class SchemaCommand {

    public static void build(String configurationFile, String outputDirectory, String password, boolean createDocs) throws Exception {
        // read ingest-configuration.yml
        Configuration ingestConfig = StreamlinerUtil.readConfigFromPath(configurationFile);
        Configuration outputConfig = null;
        if (ingestConfig.getSource().getType().equalsIgnoreCase("JDBC")) {
            if (password.equals("") || password == null) {
                throw new RuntimeException("A databasePassword is required when crawling JDBC sources");
            }
            if(createDocs){
                writeDocs(ingestConfig, password, outputDirectory);
            }
            String jdbcUrl = ingestConfig.getSource().getUrl();
            String userName = ingestConfig.getSource().getUsername();
            String schemaName = ingestConfig.getSource().getSchema();
            List<String> tableTypes = ingestConfig.getSource().getTableTypes();
            Connection con = StreamlinerUtil.getConnection(jdbcUrl, userName, password);

            SchemaDefiner schemaDef = new JdbcCrawler(jdbcUrl, () -> con, null, schemaName, tableTypes);
            StreamlinerCatalog catalog = schemaDef.retrieveSchema();
            outputConfig = StreamlinerUtil.mapJdbcCatalogToConfig(ingestConfig, catalog);
        } else if (ingestConfig.getSource().getType().equalsIgnoreCase("GLUE")) {
            SchemaDefiner schemaDef = new GlueCrawler(ingestConfig);
            StreamlinerCatalog catalog = schemaDef.retrieveSchema();
            outputConfig = StreamlinerUtil.mapGlueCatalogToConfig(ingestConfig, catalog);
        }

        //TODO: keep the parameter sequence as same as input file
        StreamlinerUtil.writeConfigToYaml(outputConfig, outputDirectory);
    }

    private static void writeDocs(Configuration ingestConfig, String password, String outputDir) throws Exception {
        if (outputDir.equals("") || outputDir == null) {
            outputDir = outputDir + "/" + ingestConfig.getName() + "/" + ingestConfig.getEnvironment() + "/docs";
        } else {
            outputDir = outputDir + "/docs";
        }
        Source source = ingestConfig.getSource();
        StreamlinerUtil.getErdOutput(source, password, outputDir);
        StreamlinerUtil.getHtmlOutput(source, password, outputDir);
    }
}
