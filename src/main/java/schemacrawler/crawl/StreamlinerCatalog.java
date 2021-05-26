package schemacrawler.crawl;

import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StreamlinerCatalog {
    private final List<Schema> schemas;
    private final Map<Schema, List<Table>> tables;
    private final String driverClassName;

    public StreamlinerCatalog(String driverClassName, List<Schema> schemas, Map<Schema, List<Table>> tables) {
        this.driverClassName = driverClassName;
        this.schemas = schemas;
        this.tables = tables;
    }

    public Collection<Schema> getSchemas() {
        return schemas;
    }

    public Collection<Table> getTables(Schema schema) {
        if (tables.containsKey(schema)) {
            return tables.get(schema);
        }
        return Collections.emptyList();
    }

    public String getDriverClassName() {
        return driverClassName;
    }
}
