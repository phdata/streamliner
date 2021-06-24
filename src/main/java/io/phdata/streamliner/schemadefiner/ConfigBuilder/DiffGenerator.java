package io.phdata.streamliner.schemadefiner.ConfigBuilder;

import io.phdata.streamliner.schemadefiner.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DiffGenerator {

  private static final Logger log = LoggerFactory.getLogger(DiffGenerator.class);

  public static ConfigurationDiff createConfigDiff(
      Configuration prevConfig, Configuration currConfig) {
    List<TableDiff> tableDiffList;
    // if prevConfig is empty, every column is ADD
    if (prevConfig == null) {
      log.info("Previous Config is Empty. Every column is an ADD");
      tableDiffList = getTableDiff(currConfig);
      return new ConfigurationDiff(
          currConfig.getName(),
          currConfig.getEnvironment(),
          currConfig.getPipeline(),
          null,
          currConfig.getDestination(),
          tableDiffList);
    } else {
      tableDiffList = getTableDiff(prevConfig, currConfig);
    }
    return new ConfigurationDiff(
        currConfig.getName(),
        currConfig.getEnvironment(),
        currConfig.getPipeline(),
        prevConfig.getDestination(),
        currConfig.getDestination(),
        tableDiffList);
  }

  private static List<TableDiff> getTableDiff(Configuration prevConfig, Configuration currConfig) {
    List<TableDiff> tableDiffList = new ArrayList<>();

    Map<String, TableDefinition> prevTableDefMap = createTableDefMap(prevConfig.getTables());
    Map<String, TableDefinition> currTableDefMap = createTableDefMap(currConfig.getTables());

    Set<String> prevTablesSet = prevTableDefMap.keySet();
    Set<String> currTablesSet = currTableDefMap.keySet();

    Set<String> deletedTables = getSetDiff(prevTablesSet, currTablesSet);
    Set<String> addedTables = getSetDiff(currTablesSet, prevTablesSet);
    Set<String> commonTables = getCommonSet(prevTablesSet, currTablesSet);

    log.info("TABLES ADDED: {}, TABLES DELETED: {}", addedTables, deletedTables);

    findCommonTablesDiff(tableDiffList, prevTableDefMap, currTableDefMap, commonTables);
    markNewTablesAdded(tableDiffList, currTableDefMap, addedTables);
    markTablesDeleted(tableDiffList, prevTableDefMap, deletedTables);

    return tableDiffList;
  }

  private static void markTablesDeleted(
      List<TableDiff> tableDiffList,
      Map<String, TableDefinition> prevTableDefMap,
      Set<String> deletedTables) {
    deletedTables.stream()
        .forEach(
            deletedTable -> {
              TableDefinition tableDef = prevTableDefMap.get(deletedTable);
              Map<String, ColumnDefinition> colDefMap = createColumnDefMap(tableDef.getColumns());
              List<ColumnDiff> columnDiffs = new ArrayList<>();
              TableDiff tableDiff;
              colDefMap.keySet().stream()
                  .forEach(
                      column -> {
                        ColumnDefinition prevColDef = colDefMap.get(column);
                        ColumnDiff colDiff = new ColumnDiff(prevColDef, null, true, false, false);
                        columnDiffs.add(colDiff);
                      });
              tableDiff =
                  new TableDiff(
                      tableDef.getType(), tableDef.getDestinationName(), columnDiffs, true, false);
              tableDiffList.add(tableDiff);
            });
  }

  private static void findCommonTablesDiff(
      List<TableDiff> tableDiffList,
      Map<String, TableDefinition> prevTableDefMap,
      Map<String, TableDefinition> currTableDefMap,
      Set<String> commonTables) {
    commonTables.stream()
        .forEach(
            commonTable -> {
              TableDefinition prevTableDef = prevTableDefMap.get(commonTable);
              TableDefinition currTableDef = currTableDefMap.get(commonTable);
              TableDiff tableDiff;
              List<ColumnDiff> columnDiffs = new ArrayList<>();

              Map<String, ColumnDefinition> prevColDefMap =
                  createColumnDefMap(prevTableDef.getColumns());
              Map<String, ColumnDefinition> currColDefMap =
                  createColumnDefMap(currTableDef.getColumns());

              // previous config all columns set
              Set<String> prevColSet = prevColDefMap.keySet();
              // current config all columns set
              Set<String> currColSet = currColDefMap.keySet();

              Set<String> deletedColumns = getSetDiff(prevColSet, currColSet);
              Set<String> addedColumns = getSetDiff(currColSet, prevColSet);
              Set<String> commonColumns = getCommonSet(prevColSet, currColSet);

              Set<String> updatedColumns = new LinkedHashSet<>();

              markColumnsDeleted(columnDiffs, prevColDefMap, deletedColumns);
              markColumnsAdded(columnDiffs, currColDefMap, addedColumns);
              findCommonColumnsDiff(
                  columnDiffs, prevColDefMap, currColDefMap, commonColumns, updatedColumns);

              log.debug(
                  "Table: {}, DeletedColumns: {}, AddedColumns: {}, UpdatedColumns: {}",
                  prevTableDef.getSourceName(),
                  deletedColumns,
                  addedColumns,
                  updatedColumns);

              if (!columnDiffs.isEmpty()) {
                tableDiff =
                    new TableDiff(
                        currTableDef.getType(),
                        currTableDef.getDestinationName(),
                        columnDiffs,
                        true,
                        true);
                tableDiffList.add(tableDiff);
              }
            });
  }

  private static void markNewTablesAdded(
      List<TableDiff> tableDiffList,
      Map<String, TableDefinition> currTableDefMap,
      Set<String> addedTables) {
    addedTables.stream()
        .forEach(
            addedTable -> {
              TableDefinition tableDef = currTableDefMap.get(addedTable);
              Map<String, ColumnDefinition> colDefMap = createColumnDefMap(tableDef.getColumns());
              List<ColumnDiff> columnDiffs = new ArrayList<>();
              TableDiff tableDiff;
              colDefMap.keySet().stream()
                  .forEach(
                      column -> {
                        ColumnDefinition currColDef = colDefMap.get(column);
                        ColumnDiff colDiff = new ColumnDiff(null, currColDef, false, true, false);
                        columnDiffs.add(colDiff);
                      });
              tableDiff =
                  new TableDiff(
                      tableDef.getType(), tableDef.getDestinationName(), columnDiffs, false, true);
              tableDiffList.add(tableDiff);
            });
  }

  private static void findCommonColumnsDiff(
      List<ColumnDiff> columnDiffs,
      Map<String, ColumnDefinition> prevColDefMap,
      Map<String, ColumnDefinition> currColDefMap,
      Set<String> commonColumns,
      Set<String> updatedColumns) {
    commonColumns.stream()
        .forEach(
            commonCol -> {
              ColumnDefinition prevColDef = prevColDefMap.get(commonCol);
              ColumnDefinition currColDef = currColDefMap.get(commonCol);
              if (!prevColDef.equals(currColDef)) {
                updatedColumns.add(prevColDef.getSourceName());
                ColumnDiff colDiff = new ColumnDiff(prevColDef, currColDef, false, false, true);
                columnDiffs.add(colDiff);
              }
            });
  }

  private static void markColumnsAdded(
      List<ColumnDiff> columnDiffs,
      Map<String, ColumnDefinition> currColDefMap,
      Set<String> addedColumns) {
    addedColumns.stream()
        .forEach(
            addedCol -> {
              ColumnDefinition currColDef = currColDefMap.get(addedCol);
              ColumnDiff colDiff = new ColumnDiff(null, currColDef, false, true, false);
              columnDiffs.add(colDiff);
            });
  }

  private static void markColumnsDeleted(
      List<ColumnDiff> columnDiffs,
      Map<String, ColumnDefinition> prevColDefMap,
      Set<String> deletedColumns) {
    deletedColumns.stream()
        .forEach(
            deletedCol -> {
              ColumnDefinition prevColDef = prevColDefMap.get(deletedCol);
              ColumnDiff colDiff = new ColumnDiff(prevColDef, null, true, false, false);
              columnDiffs.add(colDiff);
            });
  }

  private static Map<String, TableDefinition> createTableDefMap(
      List<TableDefinition> tableDefList) {
    Map<String, TableDefinition> tableDefMap = new LinkedHashMap<>();
    tableDefList.stream()
        .forEach(
            tableDef -> {
              tableDefMap.put(tableDef.getSourceName(), tableDef);
            });
    return tableDefMap;
  }

  private static Map<String, ColumnDefinition> createColumnDefMap(
      List<ColumnDefinition> colDefList) {
    Map<String, ColumnDefinition> colDefMap = new LinkedHashMap<>();
    colDefList.stream()
        .forEach(
            colDef -> {
              colDefMap.put(colDef.getSourceName(), colDef);
            });
    return colDefMap;
  }

  private static Set<String> getCommonSet(Set<String> set1, Set<String> set2) {
    Set<String> tempSet1 = new LinkedHashSet<>(set1);
    Set<String> tempSet2 = new LinkedHashSet<>(set2);
    tempSet1.retainAll(tempSet2);
    return tempSet1;
  }

  private static Set<String> getSetDiff(Set<String> set1, Set<String> set2) {
    Set<String> tempSet1 = new LinkedHashSet<>(set1);
    Set<String> tempSet2 = new LinkedHashSet<>(set2);
    tempSet1.removeAll(tempSet2);
    return tempSet1;
  }

  private static List<TableDiff> getTableDiff(Configuration currConfig) {
    List<TableDiff> tableDiffList = new ArrayList<>();
    currConfig.getTables().stream()
        .forEach(
            tableDef -> {
              List<ColumnDiff> columnDiffList = new ArrayList<>();
              tableDef.getColumns().stream()
                  .forEach(
                      colDef -> {
                        ColumnDiff colDiff = new ColumnDiff(null, colDef, false, true, false);
                        columnDiffList.add(colDiff);
                      });
              TableDiff tableDiff =
                  new TableDiff(
                      tableDef.getType(),
                      tableDef.getDestinationName(),
                      columnDiffList,
                      false,
                      true);
              tableDiffList.add(tableDiff);
            });
    return tableDiffList;
  }
}
