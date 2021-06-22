package schemacrawler.crawl;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaReference;
import schemacrawler.tools.utility.SchemaCrawlerUtility;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class StreamlinerSchemaCrawler {
    private static final Logger log = Logger.getLogger(StreamlinerSchemaCrawler.class);

    private static final String ORACLE_TABLES = "oracle-schema-crawler-tables.sql";
    private static final String ORACLE_COLUMNS = "oracle-schema-crawler-columns.sql";
    private static final String ORACLE_CONSTRAINTS = "oracle-schema-crawler-constraints.sql";
    private static final String ORACLE_UNIQUE_INDEXES = "oracle-schema-crawler-unique-indexes.sql";
    protected static final String ORACLE_TABLES_TEMPLATE;
    protected static final String ORACLE_COLUMNS_TEMPLATE;
    protected static final String ORACLE_CONSTRAINTS_TEMPLATE;
    protected static final String ORACLE_UNIQUE_INDEXES_TEMPLATE;

    private static String find(String name) {
        try (InputStream is = StreamlinerSchemaCrawler.class.getClassLoader().getResourceAsStream(name)) {
            return IOUtils.toString(is, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static {
        ORACLE_TABLES_TEMPLATE = find(ORACLE_TABLES);
        ORACLE_COLUMNS_TEMPLATE = find(ORACLE_COLUMNS);
        ORACLE_CONSTRAINTS_TEMPLATE = find(ORACLE_CONSTRAINTS);
        ORACLE_UNIQUE_INDEXES_TEMPLATE = find(ORACLE_UNIQUE_INDEXES);
    }

    private static String trimIfNotNull(String s) {
        if (s == null) return null;
        return s.trim();
    }

    private static Integer castToInteger(Object value, Integer defaultValue) {
        if (value == null) return defaultValue;
        if (value instanceof Integer) {
            return (Integer)value;
        } else if (value instanceof Number) {
            return ((Number)value).intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    protected static StreamlinerCatalog getOracleCatalog(QueryHandler queryHandler, String schemaName,
                                                       List<String> tableTypes) throws SQLException, InterruptedException {
        Map<String, MutableTable> tables = new TreeMap<>();
        Schema schema = new SchemaReference(schemaName, schemaName);
        boolean findTables = tableTypes.contains("table");
        boolean findViews = tableTypes.contains("view");
        {
            for (Map<String, Object> row : queryHandler.queryToList(ORACLE_TABLES_TEMPLATE, schemaName)) {
                String tableName = (String) row.get("TABLE_NAME");
                if (tables.containsKey(tableName)) {
                    throw new IllegalStateException("Found table " + tableName + " twice");
                }
                String remarks = trimIfNotNull((String) row.get("REMARKS"));
                String tableType = (String) row.get("TABLE_TYPE"); // TABLE, VIEW, or MATERIALIZED VIEW
                if (tableType.contains("VIEW")) {
                    if (findViews) {
                        MutableView table = new MutableView(schema, tableName);
                        table.setRemarks(remarks);
                        tables.put(tableName, table);
                    }
                } else if (findTables) {
                    MutableTable table = new MutableTable(schema, tableName);
                    table.setRemarks(remarks);
                    tables.put(tableName, table);
                }
            }
        }
        {
            for (Map<String, Object> row : queryHandler.queryToList(ORACLE_COLUMNS_TEMPLATE, schemaName)) {
                String tableName = (String) row.get("TABLE_NAME");
                String columnName = (String) row.get("COLUMN_NAME");
                String typeName = (String) row.get("TYPE_NAME");
                Integer columnSize = castToInteger(row.get("COLUMN_SIZE"), 0);
                Integer decimalDigits = castToInteger(row.get("DECIMAL_DIGITS"), 0);
                Integer ordinalPosition = castToInteger(row.get("ORDINAL_POSITION"), Integer.MAX_VALUE);
                String remarks = trimIfNotNull((String) row.get("REMARKS"));
                MutableTable table = tables.get(tableName);
                if (table != null) {
                    MutableColumn column = new MutableColumn(table, columnName);
                    column.setColumnDataType(new MutableColumnDataType(schema, typeName));
                    column.setSize(columnSize);
                    column.setDecimalDigits(decimalDigits);
                    column.setRemarks(remarks);
                    column.setOrdinalPosition(ordinalPosition);
                    table.addColumn(column);
                }
            }
        }
        {
            for (Map<String, Object> row : queryHandler.queryToList(ORACLE_CONSTRAINTS_TEMPLATE, schemaName)) {
                String tableName = (String) row.get("TABLE_NAME");
                String columnName = (String) row.get("COLUMN_NAME");
                String constraintType = (String) row.get("CONSTRAINT_TYPE");
                MutableTable table = tables.get(tableName);
                if (table != null) {
                    for (Column column : table.getColumns()) {
                        if (columnName.equalsIgnoreCase(column.getName())) {
                            MutableColumn mutableColumn = (MutableColumn) column;
                            if ("P".equalsIgnoreCase(constraintType)) {
                                mutableColumn.markAsPartOfPrimaryKey();
                            } else if ("U".equalsIgnoreCase(constraintType)) {
                                mutableColumn.markAsPartOfUniqueIndex();
                            }
                        }
                    }
                }
            }
        }
        {
            for (Map<String, Object> row : queryHandler.queryToList(ORACLE_UNIQUE_INDEXES_TEMPLATE, schemaName)) {
                String tableName = (String) row.get("TABLE_NAME");
                String columnName = (String) row.get("COLUMN_NAME");
                MutableTable table = tables.get(tableName);
                if (table != null) {
                    for (Column column : table.getColumns()) {
                        if (columnName.equalsIgnoreCase(column.getName())) {
                            MutableColumn mutableColumn = (MutableColumn) column;
                            mutableColumn.markAsPartOfUniqueIndex();
                        }
                    }
                }
            }
        }
        Map<Schema, List<Table>> result = new HashMap<>();
        result.put(schema, new ArrayList<>(tables.values()));
        return new StreamlinerCatalog("oracle.jdbc.OracleDriver", Arrays.asList(schema),
                result);
    }

    public static StreamlinerCatalog getCatalog(String schemaName, String jdbcUrl,
            final Supplier<Connection> connectionSupplier, final SchemaCrawlerOptions schemaCrawlerOptions,
            List<String> tableTypes)
            throws Exception {
        if (jdbcUrl.startsWith("jdbc:oracle")) {
            StopWatch retrieveTablesTimer = StopWatch.createStarted();
            StreamlinerCatalog catalog = getOracleCatalog(new QueryHandler(connectionSupplier), schemaName, tableTypes);
            retrieveTablesTimer.stop();
            log.info(String.format("Retrieve Tables took %s", retrieveTablesTimer.formatTime()));
            return catalog;
        }
        try (Connection connection = connectionSupplier.get()) {
            Map<Schema, List<Table>> schemaTableMap = new HashMap<>();
            Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, schemaCrawlerOptions);
            for (Schema schema : catalog.getSchemas()) {
                List<Table> tables = schemaTableMap.get(schema);
                if (tables == null) {
                    tables = new ArrayList<>();
                    schemaTableMap.put(schema, tables);
                }
                tables.addAll(catalog.getTables(schema));
            }
            return new StreamlinerCatalog(catalog.getJdbcDriverInfo().getDriverClassName(),
                    new ArrayList<>(catalog.getSchemas()), schemaTableMap);
        }
    }

    protected static class QueryHandler {
        private final Supplier<Connection> connectionSupplier;

        public QueryHandler(Supplier<Connection> connectionSupplier) {
            this.connectionSupplier = connectionSupplier;
        }

        protected List<Map<String, Object>> queryToList(String query,
                                                             String schemaName) throws SQLException, InterruptedException {
            int numThreads = 4;
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            List<Map<String, Object>> result = Collections.synchronizedList(new ArrayList<>());
            try {
                List<Future> futures = new ArrayList<>();
                for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
                    int finalThreadIndex = threadIndex;
                    futures.add(executorService.submit(() -> {
                        String queryString = query.replace("{{SCHEMA_NAME}}", schemaName)
                                .replace("{{THREAD_INDEX}}", String.valueOf(finalThreadIndex));
                        StopWatch timer = StopWatch.createStarted();
                        String queryForLogging = query.replace('\n', ' ');
                        try (Connection connection = connectionSupplier.get();
                             Statement statement = connection.createStatement()) {
                            try {
                                // I don't believe this is required to perform parallel queries across different connections
                                // but someone has suggested it's required, the query runs without incident, and has no
                                // known performance impact, so we run it but continue on if it fails.
                                statement.execute("begin " +
                                        "execute immediate 'alter session set parallel_degree_policy=auto'; " +
                                        "end;");
                            } catch (SQLException e) {
                                log.info("Unable to set parallel_degree_policy=auto, we believe this should not impact you", e);
                            }
                            log.info("Executing: " + queryForLogging);
                            ResultSet rs = statement.executeQuery(queryString);
                            ResultSetMetaData rsmd = rs.getMetaData();
                            while (rs.next()) {
                                int count = rsmd.getColumnCount();
                                Map<String, Object> row = new LinkedHashMap<>();
                                for (int i = 1; i < count + 1; i++) {
                                    row.put(rsmd.getColumnLabel(i), rs.getObject(i));
                                }
                                log.trace(row);
                                result.add(row);
                            }
                            timer.stop();
                            log.info(String.format("Thread %s took %s to execute %s", finalThreadIndex, timer.formatTime(),
                                    queryForLogging));
                            return result;
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                    }));
                }
                for (Future f : futures) {
                    try {
                        f.get();
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        if (cause != null) {
                            if (cause instanceof RuntimeException) {
                                throw (RuntimeException) cause;
                            } else if (cause instanceof SQLException) {
                                throw (SQLException) cause;
                            } else {
                                throw new RuntimeException(cause);
                            }
                        }
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                executorService.shutdown();
            }
            return result;
        }
    }
}
