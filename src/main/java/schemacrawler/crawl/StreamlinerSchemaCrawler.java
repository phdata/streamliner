package schemacrawler.crawl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.phdata.streamliner.schemadefiner.model.Jdbc;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
import java.util.stream.Collectors;

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
    private static final String SNOWFLAKE_TABLES = "snowflake-schema-crawler-tables.sql";
    private static final String SNOWFLAKE_COLUMNS = "snowflake-schema-crawler-columns.sql";
    private static final String SNOWFLAKE_SCHEMAS = "snowflake-schema-crawler-schemas.sql";
    protected static final String SNOWFLAKE_TABLES_TEMPLATE;
    protected static final String SNOWFLAKE_COLUMNS_TEMPLATE;
    protected static final String SNOWFLAKE_SCHEMAS_TEMPLATE;
    private static final String SNOWFLAKE_TABLES_SHOW_COMMAND = "snowflake-schema-crawler-tables-show-command.sql";
    private static final String SNOWFLAKE_COLUMNS_SHOW_COMMAND = "snowflake-schema-crawler-columns-show-command.sql";
    private static final String SNOWFLAKE_SCHEMAS_SHOW_COMMAND = "snowflake-schema-crawler-schemas-show-command.sql";
    protected static final String SNOWFLAKE_TABLES_TEMPLATE_SHOW_COMMAND;
    protected static final String SNOWFLAKE_COLUMNS_TEMPLATE_SHOW_COMMAND;
    protected static final String SNOWFLAKE_SCHEMAS_TEMPLATE_SHOW_COMMAND;


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
        SNOWFLAKE_TABLES_TEMPLATE = find(SNOWFLAKE_TABLES);
        SNOWFLAKE_COLUMNS_TEMPLATE = find(SNOWFLAKE_COLUMNS);
        SNOWFLAKE_SCHEMAS_TEMPLATE = find(SNOWFLAKE_SCHEMAS);
        SNOWFLAKE_TABLES_TEMPLATE_SHOW_COMMAND = find(SNOWFLAKE_TABLES_SHOW_COMMAND);
        SNOWFLAKE_COLUMNS_TEMPLATE_SHOW_COMMAND = find(SNOWFLAKE_COLUMNS_SHOW_COMMAND);
        SNOWFLAKE_SCHEMAS_TEMPLATE_SHOW_COMMAND = find(SNOWFLAKE_SCHEMAS_SHOW_COMMAND);
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
                                                       List<String> tableTypes, List<String> tableWhitelist) throws SQLException, InterruptedException {
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
                // from oracle-schema-crawler-columns.sql:
                // DECODE (COLUMNS.NULLABLE, 'N', 0, 1) AS NULLABLE, => means 0 is NO and 1 is YES
                boolean nullable = castToInteger(row.get("NULLABLE"), 1) == 1;
                MutableTable table = tables.get(tableName);
                if (table != null) {
                    MutableColumn column = new MutableColumn(table, columnName);
                    column.setColumnDataType(new MutableColumnDataType(schema, typeName));
                    column.setSize(columnSize);
                    column.setDecimalDigits(decimalDigits);
                    column.setRemarks(remarks);
                    column.setOrdinalPosition(ordinalPosition);
                    column.setNullable(nullable);
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
    if (tableWhitelist != null) {
      List<Table> tableFiltered =
          tables.values().stream()
              .filter(table -> tableWhitelist.contains(table.getName()))
              .collect(Collectors.toList());
      result.put(schema, tableFiltered);
    } else {
      result.put(schema, new ArrayList<>(tables.values()));
    }
        return new StreamlinerCatalog("oracle.jdbc.OracleDriver", Arrays.asList(schema),
                result);
    }

  public static StreamlinerCatalog getCatalog(
      Jdbc jdbc,
      final Supplier<Connection> connectionSupplier,
      final SchemaCrawlerOptions schemaCrawlerOptions,
      List<String> tableTypes)
      throws Exception {
        String jdbcUrl = jdbc.getUrl();
        String schemaName = jdbc.getSchema();
        List<String> tableWhitelist = jdbc.getTables();

        if (jdbcUrl.startsWith("jdbc:oracle")) {
            StopWatch retrieveTablesTimer = StopWatch.createStarted();
            StreamlinerCatalog catalog = getOracleCatalog(new QueryHandler(connectionSupplier), schemaName, tableTypes, tableWhitelist);
            retrieveTablesTimer.stop();
            log.info(String.format("Retrieve Tables took %s", retrieveTablesTimer.formatTime()));
            return catalog;
        }
        else if (jdbcUrl.startsWith("jdbc:snowflake")){
            // default approach for snowflake schema crawler is using INFORMATION SCHEMA.
          String schemaCrawlerApproach =
                  (jdbc.getSnowflakeCrawler() == null) ? "information_schema" : jdbc.getSnowflakeCrawler();
          if (schemaCrawlerApproach.equalsIgnoreCase("show_command")) {
              log.info("Snowflake schema crawler will use SHOW COMMANDS.");
              StopWatch retrieveTablesTimer = StopWatch.createStarted();
              StreamlinerCatalog catalog =
                      getSnowflakeCatalogUsingShowCommand(
                              new SnowflakeQueryHandler(connectionSupplier),
                              schemaName,
                              tableTypes,
                              tableWhitelist);
              retrieveTablesTimer.stop();
              log.info(String.format("Retrieve Tables took %s", retrieveTablesTimer.formatTime()));
              return catalog;
          } else if (schemaCrawlerApproach.equalsIgnoreCase("information_schema")) {
              log.info("Snowflake schema crawler will use INFORMATION SCHEMA.");
            StopWatch retrieveTablesTimer = StopWatch.createStarted();
            StreamlinerCatalog catalog =
                getSnowflakeCatalogUsingInformationSchema(
                    new SnowflakeQueryHandler(connectionSupplier),
                    schemaName,
                    tableTypes,
                    tableWhitelist,
                    jdbc.getBatchTableCount());
            retrieveTablesTimer.stop();
            log.info(String.format("Retrieve Tables took %s", retrieveTablesTimer.formatTime()));
            return catalog;
          } else {
            throw new RuntimeException(
                String.format(
                    "Invalid snowflake schema crawler approach '%s' provided. Valid values are show_command/information_schema.", schemaCrawlerApproach));
          }
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

  private static StreamlinerCatalog getSnowflakeCatalogUsingShowCommand(
      SnowflakeQueryHandler queryHandler,
      String schemaName,
      List<String> tableTypes,
      List<String> tableWhitelist)
      throws SQLException {

    String database = queryHandler.getCatalog();
    Map<String, MutableTable> tables = new TreeMap<>();
    Schema schema = new SchemaReference(database, schemaName);
    boolean findTables = tableTypes.contains("table");
    boolean findViews = tableTypes.contains("view");

    List<String> tableList = new ArrayList<>();
    // This will check if schema name provided in config is valid or not.
    {
      List<Map<String, Object>> schemas =
          queryHandler.queryToList(SNOWFLAKE_SCHEMAS_TEMPLATE_SHOW_COMMAND, "", "");
      boolean isSchemaValid =
          schemas.stream()
              .anyMatch(
                  row -> {
                    if (((String) row.get("name")).equalsIgnoreCase(schemaName)) {
                      return true;
                    } else {
                      return false;
                    }
                  });
      if (!isSchemaValid) {
        throw new IllegalStateException(
            String.format("%s database does not have %s schema.", database, schemaName));
      }
    }

    {
      for (Map<String, Object> row :
          queryHandler.queryToList(SNOWFLAKE_TABLES_TEMPLATE_SHOW_COMMAND, schemaName, "")) {
        String tableName = (String) row.get("name");
        tableList.add(tableName);
        if (tables.containsKey(tableName)) {
          throw new IllegalStateException(String.format("Found table %s twice", tableName));
        }
        String comment = trimIfNotNull((String) row.get("comment"));
        String tableType = (String) row.get("kind");
        if (tableType.contains("VIEW")) {
          if (findViews) {
            MutableView table = new MutableView(schema, tableName);
            table.setRemarks(comment);
            tables.put(tableName, table);
          }
        } else if (findTables) {
          MutableTable table = new MutableTable(schema, tableName);
          table.setRemarks(comment);
          tables.put(tableName, table);
        }
      }
    }
    {
        log.info(String.format("Snowflake have %d tables.", tableList.size()));
        log.debug(String.format("Snowflake tables: %s", tableList));
      tableList.stream()
          .forEach(
              snowflakeTable -> {
                List<Map<String, Object>> columnsInfo =
                    queryHandler.queryToList(
                        SNOWFLAKE_COLUMNS_TEMPLATE_SHOW_COMMAND, schemaName, snowflakeTable);
                Integer colOrdinalPos = 1;
                for (Map<String, Object> row : columnsInfo) {
                  String tableName = (String) row.get("table_name");
                  String columnName = (String) row.get("column_name");

                  // dataTypeInfo provides information about precision, scale, nullability etc
                  Map<String, Object> dataTypeInfo = getDataType(row);
                   // SHOW command returns NUMBER data type as FIXED.
                  String typeName = dataTypeInfo.get("type").equals("FIXED")? "NUMBER" :(String) dataTypeInfo.get("type");

                  /* TODO:  SHOW COLUMN command gives precision and scale value for TIMESTAMP data type. Where as Information.columns gives  DATETIME_PRECISION for precision.
                  * Observed that value of Information.columns DATETIME_PRECISION is same as SHOW COLUMN command scale value.
                  *  Need to cross check that is the values getting reversed if schema crawler approach is changed from INFORMATION SCHEMA to SHOW COMMAND.
                  * */
                  Integer columnSize = castToInteger(getColumnPrecision(dataTypeInfo), 0);
                  Integer decimalDigits = castToInteger(getColumnScale(dataTypeInfo), 0);

                  // observed that SHOW COLUMN command list the columns according to ordinal position. Hence increasing colOrdinalPos in every iteration and assigning to ordinalPosition
                  Integer ordinalPosition = castToInteger(colOrdinalPos++, Integer.MAX_VALUE);
                  String remarks = trimIfNotNull((String) row.get("comment"));
                  boolean nullable = (boolean) dataTypeInfo.get("nullable");
                  MutableTable table = tables.get(tableName);
                  if (table != null) {
                    MutableColumn column = new MutableColumn(table, columnName);
                    column.setColumnDataType(new MutableColumnDataType(schema, typeName));
                    column.setSize(columnSize);
                    column.setDecimalDigits(decimalDigits);
                    column.setRemarks(remarks);
                    column.setOrdinalPosition(ordinalPosition);
                    column.setNullable(nullable);
                    table.addColumn(column);
                  }
                }
              });
    }
    Map<Schema, List<Table>> result = new HashMap<>();
    if (tableWhitelist != null) {
      List<Table> tableFiltered =
          tables.values().stream()
              .filter(table -> tableWhitelist.contains(table.getName()))
              .collect(Collectors.toList());
      result.put(schema, tableFiltered);
    } else {
      result.put(schema, new ArrayList<>(tables.values()));
    }
    return new StreamlinerCatalog(
        "net.snowflake.client.jdbc.SnowflakeDriver", Arrays.asList(schema), result);
  }

    private static Integer getColumnScale(Map<String, Object> dataTypeInfo) {
        String dataType = (String)dataTypeInfo.get("type");
        if(dataType.equals("FIXED")){
            return (Integer)dataTypeInfo.get("scale");
        } else if ( dataType.startsWith("TIMESTAMP")){
            return (Integer)dataTypeInfo.get("scale");
        } else {
            return 0;
        }
    }

    private static Integer getColumnPrecision(Map<String, Object> dataTypeInfo) {
        String dataType = (String)dataTypeInfo.get("type");
        if(dataType.equals("FIXED")){
            return (Integer)dataTypeInfo.get("precision");
        } else if ( dataType.equals("TEXT")){
            return (Integer)dataTypeInfo.get("length");
        } else if ( dataType.startsWith("TIMESTAMP")){
            return (Integer)dataTypeInfo.get("precision");
        } else {
            return 0;
        }
    }

    private static Map<String, Object> getDataType(Map<String, Object> row) {
        ObjectMapper mapper = new ObjectMapper();
        try {
             return  mapper.readValue((String) row.get("data_type"), Map.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting String json to Map", e);
        }
    }

    private static StreamlinerCatalog getSnowflakeCatalogUsingInformationSchema(
          SnowflakeQueryHandler queryHandler,
          String schemaName,
          List<String> tableTypes,
          List<String> tableWhitelist, int batchTableCount) throws SQLException {
    String database = queryHandler.getCatalog();
    Map<String, MutableTable> tables = new TreeMap<>();
    Schema schema = new SchemaReference(database, schemaName);
    boolean findTables = tableTypes.contains("table");
    boolean findViews = tableTypes.contains("view");

    // default value for batch query count is 10.
    batchTableCount = batchTableCount == 0 ? 10 : batchTableCount;
    String snowflakeSchemaName = "";
    List<String> tableList = new ArrayList<>();

    // This will check if schema name provided in config is valid or not.
    {
      List<Map<String, Object>> schemas =
          queryHandler.queryToList(SNOWFLAKE_SCHEMAS_TEMPLATE, "", "");
      boolean isSchemaValid =
          schemas.stream()
              .anyMatch(
                  row -> {
                    if (((String) row.get("SCHEMA_NAME")).equalsIgnoreCase(schemaName)) {
                      return true;
                    } else {
                      return false;
                    }
                  });
      if (!isSchemaValid) {
        throw new IllegalStateException(
            String.format("%s database does not have %s schema.", database, schemaName));
      }
    }

    {
      for (Map<String, Object> row :
          queryHandler.queryToList(SNOWFLAKE_TABLES_TEMPLATE, schemaName, "")) {
        snowflakeSchemaName = (String) row.get("TABLE_SCHEMA");
        String tableName = (String) row.get("TABLE_NAME");
        tableList.add(String.format("'%s'", tableName));
        if (tables.containsKey(tableName)) {
          throw new IllegalStateException(String.format("Found table %s twice", tableName));
        }
        String comment = trimIfNotNull((String) row.get("COMMENT"));
        String tableType = (String) row.get("TABLE_TYPE");
        if (tableType.contains("VIEW")) {
          if (findViews) {
            MutableView table = new MutableView(schema, tableName);
            table.setRemarks(comment);
            tables.put(tableName, table);
          }
        } else if (findTables) {
          MutableTable table = new MutableTable(schema, tableName);
          table.setRemarks(comment);
          tables.put(tableName, table);
        }
      }
    }
    {
      int snowflakeTablesCount = tableList.size();
      log.info(String.format("Snowflake have %d tables.", snowflakeTablesCount));
      log.debug(String.format("Snowflake tables: %s", tableList));
      for (int i = 0; i < snowflakeTablesCount; i = i + batchTableCount) {
        List<String> subTableList = tableList.subList(i, Math.min(snowflakeTablesCount, i + batchTableCount));

        for (Map<String, Object> row :
            queryHandler.queryToList(
                SNOWFLAKE_COLUMNS_TEMPLATE, snowflakeSchemaName, StringUtils.join(subTableList, ","))) {
          String tableName = (String) row.get("TABLE_NAME");
          String columnName = (String) row.get("COLUMN_NAME");
          String dataType = (String) row.get("DATA_TYPE");
          Integer columnSize = castToInteger(row.get("PRECISION"), 0);
          Integer decimalDigits = castToInteger(row.get("SCALE"), 0);
          Integer ordinalPosition = castToInteger(row.get("ORDINAL_POSITION"), Integer.MAX_VALUE);
          String comment = trimIfNotNull((String) row.get("COMMENT"));
          boolean nullable = row.get("IS_NULLABLE").equals("YES");
          MutableTable table = tables.get(tableName);
          if (table != null) {
            MutableColumn column = new MutableColumn(table, columnName);
            column.setColumnDataType(new MutableColumnDataType(schema, dataType));
            column.setSize(columnSize);
            column.setDecimalDigits(decimalDigits);
            column.setRemarks(comment);
            column.setOrdinalPosition(ordinalPosition);
            column.setNullable(nullable);
            table.addColumn(column);
          }
        }
      }
    }
    Map<Schema, List<Table>> result = new HashMap<>();
    if (tableWhitelist != null) {
      List<Table> tableFiltered =
          tables.values().stream()
              .filter(table -> tableWhitelist.contains(table.getName()))
              .collect(Collectors.toList());
      result.put(schema, tableFiltered);
    } else {
      result.put(schema, new ArrayList<>(tables.values()));
    }
    return new StreamlinerCatalog(
        "net.snowflake.client.jdbc.SnowflakeDriver", Arrays.asList(schema), result);
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
                        String queryForLogging = queryString.replace('\n', ' ');
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

  protected static class SnowflakeQueryHandler {
    private final Connection connection;

    public SnowflakeQueryHandler(Supplier<Connection> connectionSupplier) {
      this.connection = connectionSupplier.get();
    }

    protected String getCatalog() throws SQLException {
        return connection.getCatalog();
    }

    protected List<Map<String, Object>> queryToList(
        String query, String schemaName, String tables) {
      List<Map<String, Object>> result = Collections.synchronizedList(new ArrayList<>());
      String queryString =
          query.replace("{{SCHEMA_NAME}}", schemaName).replace("{{TABLE}}", tables);
      StopWatch timer = StopWatch.createStarted();
      String queryForLogging = queryString.replace('\n', ' ');
      try (Statement statement = connection.createStatement()) {
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
        log.info(String.format("Took %s to execute %s", timer.formatTime(), queryForLogging));
        return result;
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
