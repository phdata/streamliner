package io.phdata.streamliner.schemadefiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.crawl.StreamlinerCatalog;
import schemacrawler.crawl.StreamlinerSchemaCrawler;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;

import java.sql.Connection;
import java.util.List;
import java.util.function.Supplier;

public class JdbcCrawler implements SchemaDefiner {

    private static final Logger log = LoggerFactory.getLogger(JdbcCrawler.class);

    private String user;
    private String password;
    private String jdbcUrl;
    private final Supplier<Connection> connectionSupplier;
    private final SchemaCrawlerOptions schemaCrawlerOptions;
    private String schemaName;
    private List<String> tableTypes;

    public JdbcCrawler(String jdbcUrl, Supplier<Connection> connectionSupplier, SchemaCrawlerOptions schemaCrawlerOptions, String schemaName, List<String> tableTypes) {
        this.jdbcUrl = jdbcUrl;
        this.connectionSupplier = connectionSupplier;
        this.schemaCrawlerOptions = schemaCrawlerOptions;
        this.schemaName = schemaName;
        this.tableTypes = tableTypes;
    }

    @Override
    public StreamlinerCatalog retrieveSchema() {
        try {
            return StreamlinerSchemaCrawler.getCatalog(schemaName, jdbcUrl, connectionSupplier, schemaCrawlerOptions, tableTypes);
        } catch (Exception e) {
            log.error("Error in JdbcCrawler: {}", e.getMessage());
            throw new RuntimeException("Error in JdbcCrawler: {}", e);
        }

    }


}
