package io.phdata.streamliner.schemadefiner;

import io.phdata.streamliner.schemadefiner.model.Jdbc;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
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
    private Supplier<Connection> conSupplier =
      new Supplier<Connection>() {
        @Override
        public Connection get() {
          return StreamlinerUtil.getConnection(jdbcUrl, user, password);
        }
      };

    public JdbcCrawler(Jdbc jdbc, String password) {
        this.jdbcUrl = jdbc.getUrl();
        this.user = jdbc.getUsername();
        this.password = password;
        this.schemaName = jdbc.getSchema();
        this.tableTypes = jdbc.getTableTypes();
        this.connectionSupplier = conSupplier;
        this.schemaCrawlerOptions = StreamlinerUtil.getOptions(jdbc);

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
