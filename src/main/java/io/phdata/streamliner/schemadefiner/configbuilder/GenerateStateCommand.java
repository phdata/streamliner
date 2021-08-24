package io.phdata.streamliner.schemadefiner.configbuilder;

import com.opencsv.bean.CsvToBeanBuilder;
import io.phdata.streamliner.schemadefiner.model.*;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GenerateStateCommand {
  private static final Logger log = LoggerFactory.getLogger(GenerateStateCommand.class);

  public static void build(
      String tableExclude,
      String tableInclude,
      String columnInclude,
      String columnExclude,
      String tableNameRemove,
      String columnNameRemove,
      String outputPath,
      String sourceSystemFile,
      String tableCsvPath,
      String columnCsvPath) {

    List<CSVTables> csvTables;
    List<CSVColumns> csvColumns;
    try {
      csvTables =
          new CsvToBeanBuilder(new FileReader(tableCsvPath))
              .withType(CSVTables.class)
              .build()
              .parse();

      csvColumns =
          new CsvToBeanBuilder(new FileReader(columnCsvPath))
              .withType(CSVColumns.class)
              .build()
              .parse();
    } catch (FileNotFoundException e) {
      throw new RuntimeException("File not found.", e);
    }

    Configuration sourceSchemaFile = StreamlinerUtil.readConfigFromPath(sourceSystemFile);
    // log table count and any missing tables.
    logTablesDetail(csvTables, sourceSchemaFile.getTables());

    /*
    Assuming at a time either tableExclude or tableInclude will be applied.
    This will exlude or include the tables based on regex provided.
    */
    if (tableExclude != null) {
      excludeTables(csvTables, tableExclude);
    } else if (tableInclude != null) {
      includeTables(csvTables, tableInclude);
    }

    // This will remove the regex from table name.
    if (tableNameRemove != null) {
      removeTableName(csvTables, tableNameRemove);
    }

    Map<String, TableDefinition> sourceTableDefMap =
        createTableDefMap(sourceSchemaFile.getTables());
    TemplateContext context = new TemplateContext();

    // schema crawler sorts by lower case name so we ensure that behavior here
    csvTables.sort(Comparator.comparing(csvTable -> csvTable.getTableName().toLowerCase()));

    List<TableDefinition> tableDef =
        csvTables.stream()
            .map(
                csvTable -> {
                  TableDefinition sourceTableDef = sourceTableDefMap.get(csvTable.getTableName());
                  // Stores table name which are not in source schema. At the end of run log will be
                  // printed.
                  if (sourceTableDef == null) {
                    context.addError(
                        String.format(
                            "Table '%s' not found in Source schema. Unable to get the Primary key for this table.",
                            csvTable.getTableName()));
                  }
                  Map<String, ColumnDefinition> sourceColDefMap = null;
                  if (sourceTableDef != null) {
                    sourceColDefMap = createColumnDefMap(sourceTableDef.getColumns());
                  }
                  Map<String, ColumnDefinition> finalSourceColDefMap = sourceColDefMap;
                  // iterate csvColumns and filter all the columns related to a table.
                  List<CSVColumns> csvTableCols =
                      csvColumns.stream()
                          .filter(
                              csvColumn -> csvColumn.getTableName().equals(csvTable.getTableName()))
                          .collect(Collectors.toList());
                  // columns are sorted based on ordinal position
                  csvTableCols.sort(
                      Comparator.comparing(
                          column -> Integer.parseInt(column.getOrdinalPosition())));
                  // log columns count and any missing columns.
                  if (finalSourceColDefMap != null) {
                    logColumnsDetail(csvTable, sourceTableDef, csvTableCols);
                  }

                  /*
                  Assuming at a time either columnExclude or columnInclude will be applied.
                  This will exlude or include the columns based on regex provided.
                  */
                  if (columnExclude != null) {
                    excludeColumns(csvTable, csvTableCols, columnExclude);
                  } else if (columnInclude != null) {
                    includeColumns(csvTable, csvTableCols, columnInclude);
                  }

                  // This will remove the regex from column name.
                  if (columnNameRemove != null) {
                    removeColumnName(csvTable, csvTableCols, columnNameRemove);
                  }

                  /* In case snowflake table does not exist in source schema, existing snowflake dataType, precision, scale and empty primary key is stored in state file.
                   * In case some columns are missing from source schema, existing snowflake dataType, precision, scale is stored in state file.
                   * Else values are picked from source schema file.*/
                  List<ColumnDefinition> d =
                      csvTableCols.stream()
                          .map(
                              csvCol -> {
                                String dataType =
                                    finalSourceColDefMap != null
                                        ? finalSourceColDefMap.get(csvCol.getColumnName()) == null
                                            ? csvCol.getDataType()
                                            : finalSourceColDefMap
                                                .get(csvCol.getColumnName())
                                                .getDataType()
                                        : csvCol.getDataType();

                                Integer precision =
                                    finalSourceColDefMap != null
                                        ? finalSourceColDefMap.get(csvCol.getColumnName()) == null
                                            ? Integer.parseInt(csvCol.getPrecision())
                                            : finalSourceColDefMap
                                                .get(csvCol.getColumnName())
                                                .getPrecision()
                                        : Integer.parseInt(csvCol.getPrecision());

                                Integer scale =
                                    finalSourceColDefMap != null
                                        ? finalSourceColDefMap.get(csvCol.getColumnName()) == null
                                            ? Integer.parseInt(csvCol.getScale())
                                            : finalSourceColDefMap
                                                .get(csvCol.getColumnName())
                                                .getScale()
                                        : Integer.parseInt(csvCol.getScale());

                                return new ColumnDefinition(
                                    csvCol.getColumnName(),
                                    csvCol.getColumnName(),
                                    dataType,
                                    csvCol.getComment(),
                                    precision,
                                    scale,
                                    csvCol.getIsNullable().equals("YES"));
                              })
                          .collect(Collectors.toList());

                  return new SnowflakeTable(
                      csvTable.getTableName(),
                      csvTable.getTableName(),
                      csvTable.getTableComment(),
                      sourceTableDef != null ? sourceTableDef.getPrimaryKeys() : new ArrayList<>(),
                      null,
                      null,
                      null,
                      new FileFormat(csvTable.getTableName(), "PARQUET"),
                      d);
                })
            .collect(Collectors.toList());
    Configuration config =
        new Configuration(
            sourceSchemaFile.getName(),
            sourceSchemaFile.getEnvironment(),
            sourceSchemaFile.getPipeline(),
            sourceSchemaFile.getSource(),
            sourceSchemaFile.getDestination(),
            tableDef);
    StreamlinerUtil.writeConfigToYaml(
        config, StreamlinerUtil.getOutputDirectory(outputPath), outputPath);

    if (context.hasErrors()) {
      List<String> errors = context.getErrors();
      String msg = String.format("There are %d errors which require investigation.", errors.size());
      errors.forEach(log::error);
      log.warn(msg);
    }
  }

  private static void removeColumnName(
      CSVTables csvTable, List<CSVColumns> csvTableCols, String columnNameRemoveRegex) {
    Set<String> columns = new HashSet<>();
    csvTableCols.forEach(
        csvColumn -> {
          StringBuffer sb =
              removeRegexFromString(columnNameRemoveRegex, columns, csvColumn.getColumnName());
          csvColumn.setColumnName(sb.toString());
        });
    log.info(
        "Action: {}, Regex: {} ,Table: {}, Columns: {}",
        "COLUMN NAME REMOVE",
        columnNameRemoveRegex,
        csvTable.getTableName(),
        columns);
  }

  private static void removeTableName(List<CSVTables> csvTables, String tableNameRemoveRegex) {
    Set<String> tables = new HashSet<>();
    csvTables.forEach(
        csvTable -> {
          StringBuffer sb =
              removeRegexFromString(tableNameRemoveRegex, tables, csvTable.getTableName());
          csvTable.setTableName(sb.toString());
        });
    log.info("Action: {}, Regex: {} ,Table: {}", "TABLE NAME REMOVE", tableNameRemoveRegex, tables);
  }

  private static StringBuffer removeRegexFromString(
      String tableNameRemoveRegex, Set<String> distinctTableOrColumn, String tableOrColumn) {
    boolean regexExists = true;
    StringBuffer sb = new StringBuffer();
    while (regexExists) {
      Matcher m =
          Pattern.compile(tableNameRemoveRegex, Pattern.CASE_INSENSITIVE).matcher(tableOrColumn);
      regexExists = m.find();
      if (regexExists) {
        distinctTableOrColumn.add(tableOrColumn);
        sb.append(tableOrColumn.substring(0, m.start()));
        tableOrColumn = tableOrColumn.substring(m.end());
      }
    }
    sb.append(tableOrColumn);
    return sb;
  }

  private static void includeColumns(
      CSVTables csvTable, List<CSVColumns> csvTableCols, String columnIncludeRegex) {
    List<CSVColumns> includedColumns = filterCSVColumns(csvTableCols, columnIncludeRegex);
    List<String> includedColumnsName =
        includedColumns.stream().map(CSVColumns::getColumnName).collect(Collectors.toList());

    log.info(
        "Action: {}, Regex: {} ,Table: {}, Columns count: {}, Columns: {}",
        "COLUMN INCLUDE",
        columnIncludeRegex,
        csvTable.getTableName(),
        includedColumns.size(),
        includedColumnsName);
    csvTableCols.retainAll(includedColumns);
  }

  private static void excludeColumns(
      CSVTables csvTable, List<CSVColumns> csvColumns, String columnsExcludeRegex) {
    List<CSVColumns> excludedColumns = filterCSVColumns(csvColumns, columnsExcludeRegex);
    List<String> excludedColumnsName =
        excludedColumns.stream().map(CSVColumns::getColumnName).collect(Collectors.toList());

    log.info(
        "Action: {}, Regex: {} ,Table: {}, Columns count: {}, Columns: {}",
        "COLUMN EXCLUDE",
        columnsExcludeRegex,
        csvTable.getTableName(),
        excludedColumns.size(),
        excludedColumnsName);
    csvColumns.removeAll(excludedColumns);
  }

  private static List<CSVColumns> filterCSVColumns(List<CSVColumns> csvColumns, String regex) {
    return csvColumns.stream()
        .filter(csvColumn -> applyRegex(regex, csvColumn.getColumnName()))
        .collect(Collectors.toList());
  }

  private static void includeTables(List<CSVTables> csvTables, String tableIncludeRegex) {
    List<CSVTables> includedTables = filterCSVTables(csvTables, tableIncludeRegex);
    List<String> includeTablesName =
        includedTables.stream().map(CSVTables::getTableName).collect(Collectors.toList());

    log.info(
        "Action: {}, Regex: {} , Tables count: {}, Tables: {}",
        "TABLE INCLUDE",
        tableIncludeRegex,
        includedTables.size(),
        includeTablesName);
    csvTables.retainAll(includedTables);
  }

  private static void excludeTables(List<CSVTables> csvTables, String tableExcludeRegex) {
    List<CSVTables> excludedTables = filterCSVTables(csvTables, tableExcludeRegex);
    List<String> excludeTablesName =
        excludedTables.stream().map(CSVTables::getTableName).collect(Collectors.toList());

    log.info(
        "Action: {}, Regex: {} , Tables count: {}, Tables: {}",
        "TABLE EXCLUDE",
        tableExcludeRegex,
        excludedTables.size(),
        excludeTablesName);
    csvTables.removeAll(excludedTables);
  }

  private static List<CSVTables> filterCSVTables(List<CSVTables> csvTables, String regex) {
    return csvTables.stream()
        .filter(csvTable -> applyRegex(regex, csvTable.getTableName()))
        .collect(Collectors.toList());
  }

  private static boolean applyRegex(String regex, String matcher) {
    Matcher m = Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(matcher);
    return m.find();
  }

  private static void logColumnsDetail(
      CSVTables csvTable, TableDefinition sourceTableDef, List<CSVColumns> csvTableCols) {
    List<String> sourceCols =
        sourceTableDef.getColumns().stream()
            .map(ColumnDefinition::getSourceName)
            .collect(Collectors.toList());
    List<String> csvCols =
        csvTableCols.stream().map(CSVColumns::getColumnName).collect(Collectors.toList());

    int sourceColumnCount = sourceCols.size();
    int snowflakeColumnCount = csvCols.size();
    log.debug(
        "Table {} have {} columns in Snowflake and {} in  Source schema",
        sourceTableDef.getSourceName(),
        snowflakeColumnCount,
        sourceColumnCount);

    if (snowflakeColumnCount > sourceColumnCount) {
      log.debug(
          "Table {} have {} columns missing from Source schema.",
          csvTable.getTableName(),
          getListDiff(csvCols, sourceCols));
    } else if (sourceColumnCount > snowflakeColumnCount) {
      log.debug(
          "Table {} have {} columns missing from Snowflake.",
          sourceTableDef.getSourceName(),
          getListDiff(sourceCols, csvCols));
    } else {
      log.debug(
          "Table {} have equal number of columns in Snowflake and Source schema.",
          sourceTableDef.getSourceName());
    }
  }

  private static void logTablesDetail(
      List<CSVTables> csvTables, List<TableDefinition> sourceSchemaTables) {
    List<String> sourceTableList =
        sourceSchemaTables.stream()
            .map(TableDefinition::getSourceName)
            .collect(Collectors.toList());
    List<String> csvTableList =
        csvTables.stream().map(CSVTables::getTableName).collect(Collectors.toList());

    int sourceSchemaTableCount = sourceTableList.size();
    int csvTableCount = csvTableList.size();
    log.info(
        "Number of tables in Snowflake: {}, Source schema: {}",
        csvTableCount,
        sourceSchemaTableCount);

    if (sourceSchemaTableCount > csvTableCount) {
      log.debug(
          "{} tables missing from Snowflake: {} ",
          (sourceSchemaTableCount - csvTableCount),
          getListDiff(sourceTableList, csvTableList));
    } else if (csvTableCount > sourceSchemaTableCount) {
      log.debug(
          "{} tables missing from Source Schema: {} ",
          (csvTableCount - sourceSchemaTableCount),
          getListDiff(csvTableList, sourceTableList));
    } else {
      log.debug("Source schema and Snowflake have equal numbers of tables.");
    }
  }

  private static Map<String, TableDefinition> createTableDefMap(
      List<TableDefinition> tableDefList) {
    if (tableDefList == null) return null;
    Map<String, TableDefinition> tableDefMap = new LinkedHashMap<>();
    tableDefList.forEach(tableDef -> tableDefMap.put(tableDef.getSourceName(), tableDef));
    return tableDefMap;
  }

  private static Map<String, ColumnDefinition> createColumnDefMap(
      List<ColumnDefinition> colDefList) {
    if (colDefList == null) return null;
    Map<String, ColumnDefinition> colDefMap = new LinkedHashMap<>();
    colDefList.forEach(colDef -> colDefMap.put(colDef.getSourceName(), colDef));
    return colDefMap;
  }

  private static List<String> getListDiff(List<String> list1, List<String> list2) {
    List<String> tempList1 = new ArrayList<>(list1);
    List<String> tempList2 = new ArrayList<>(list2);
    tempList1.removeAll(tempList2);
    return tempList1;
  }
}
