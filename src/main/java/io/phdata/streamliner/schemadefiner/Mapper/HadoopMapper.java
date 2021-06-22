package io.phdata.streamliner.schemadefiner.Mapper;

import io.phdata.streamliner.schemadefiner.model.*;
import schemacrawler.schema.Column;
import schemacrawler.schema.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HadoopMapper {
    public static List<TableDefinition> mapAWSGlueTables(List<Table> glueTables, List<UserDefinedTable> userDefinedTables) {
        List<HadoopTable> glueTableList = glueTables.stream().map(table -> mapAWSGlueTable(table)).collect(Collectors.toList());
    return userTableDefinitions(glueTableList, userDefinedTables).stream()
        .map(userTable -> (TableDefinition) userTable)
        .collect(Collectors.toList());
    }

    private static List<HadoopTable> userTableDefinitions(List<HadoopTable> tables, List<UserDefinedTable> userDefTables) {
        if (userDefTables != null && !userDefTables.isEmpty()) {
      List<HadoopUserDefinedTable> userTables =
          userDefTables.stream()
              .map(userTable -> (HadoopUserDefinedTable) userTable)
              .collect(Collectors.toList());
            return tables.stream().map(table -> {
                userTables.stream().forEach(userTable -> {
                    if (userTable.getName().equalsIgnoreCase(table.getSourceName())) {
                        if(table.getMetadata() != null){
                            if(userTable.getMetadata() != null){
                                table.getMetadata().putAll(userTable.getMetadata());
                            }else{
                                table.setMetadata(null);
                            }
                        }else{
                            table.setMetadata(userTable.getMetadata());
                        }

                        if(userTable.getPrimaryKeys() != null && !userTable.getPrimaryKeys().isEmpty()){
                            table.setPrimaryKeys(userTable.getPrimaryKeys());
                        }
                        table.setCheckColumn(userTable.getCheckColumn());
                        table.setNumberOfMappers(userTable.getNumberOfMappers());
                        table.setSplitByColumn(userTable.getSplitByColumn());
                        table.setNumberOfPartitions(userTable.getNumberOfPartitions());
                    }
                });
                return table;
            }).collect(Collectors.toList());
        } else {
            return tables;
        }
    }

  private static HadoopTable mapAWSGlueTable(Table table) {
    return new HadoopTable(
        table.getName(),
        table.getName(),
        null,
        table.getRemarks(),
        new ArrayList<>(),
        null,
        1,
        null,
        1,
        mapAWSGlueColumnDefinition(table.getColumns()));
    }

    private static List<ColumnDefinition> mapAWSGlueColumnDefinition(List<Column> columns) {
        return columns.stream().map(column -> new ColumnDefinition(
                column.getName(),
                column.getName(),
                column.getColumnDataType().getName(),
                column.getRemarks()
        )).collect(Collectors.toList());
    }

    public static List<TableDefinition> mapSchemaCrawlerTables(List<Table> tables, List<UserDefinedTable> userDefinedTable) {
        List<HadoopTable> tableList = tables.stream().map(table -> mapSchemaCrawlerTable(table)).collect(Collectors.toList());
    return userTableDefinitions(tableList, userDefinedTable).stream()
        .map(userTable -> (TableDefinition) userTable)
        .collect(Collectors.toList());
    }

    private static HadoopTable mapSchemaCrawlerTable(Table table) {
        List<String> primaryKeys = table.getColumns().stream().filter(c -> c.isPartOfPrimaryKey()).collect(Collectors.toList())
                .stream().map(d -> d.getName()).collect(Collectors.toList());

    return new HadoopTable(
        table.getName(),
        table.getName(),
        null,
        table.getRemarks(),
        primaryKeys,
        null,
        1,
        null,
        1,
        mapSchemaCrawlerColumnDefinition(table.getColumns()));
    }

    private static List<ColumnDefinition> mapSchemaCrawlerColumnDefinition(List<Column> columns) {
        return  columns.stream().map(column -> new ColumnDefinition(
                column.getName(),
                column.getName(),
                column.getColumnDataType().getName(),
                column.getRemarks(),
                column.getSize(),
                column.getDecimalDigits())).collect(Collectors.toList());
    }
}
