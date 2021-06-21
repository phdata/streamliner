package io.phdata.streamliner.schemadefiner.Mapper;

import io.phdata.streamliner.schemadefiner.GlueCrawler;
import io.phdata.streamliner.schemadefiner.model.*;
import schemacrawler.schema.Column;
import schemacrawler.schema.Table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SnowflakeMapper {
    public static List<TableDefinition> mapSchemaCrawlerTables(List<Table> tables, List<UserDefinedTable> userDefinedTables) {
        // schema crawler sorts by lower case name so we ensure that behavior here
        List<SnowflakeTable> sortedTableDef = tables.stream().sorted(Comparator.comparing(table-> table.getName().toLowerCase())).map(table -> mapSchemaCrawlerTable(table)).collect(Collectors.toList());
    return userTableDefinitions(sortedTableDef, userDefinedTables).stream()
        .map(userTable -> (TableDefinition) userTable)
        .collect(Collectors.toList());
    }

    private static List<SnowflakeTable> userTableDefinitions(List<SnowflakeTable> tables, List<UserDefinedTable> userDefTables) {
        if (userDefTables != null && !userDefTables.isEmpty()) {
      List<SnowflakeUserDefinedTable> userTables =
          userDefTables.stream()
              .map(userTable -> (SnowflakeUserDefinedTable) userTable)
              .collect(Collectors.toList());
            return tables.stream().map(table -> {
                userTables.stream().forEach(userTable -> {
                    if (userTable.getName().equalsIgnoreCase(table.getSourceName())) {
                        if (userTable.getFileFormat() != null) {
                            table.setFileFormat(userTable.getFileFormat());
                        }
                        if (!userTable.getPrimaryKeys().isEmpty()) {
                            table.setPrimaryKeys(userTable.getPrimaryKeys());
                        }
                    }

                });
                return table;
            }).collect(Collectors.toList());
        } else {
            return tables;
        }
    }

    private static SnowflakeTable mapSchemaCrawlerTable(Table table) {
        // columns are sorted based on ordinal position
        table.getColumns().sort(Comparator.comparing(column -> column.getOrdinalPosition()));
        List<Column> columns = table.getColumns();
        List<Column> parsedPrimaryKeys = columns.stream().filter(c->c.isPartOfPrimaryKey()).collect(Collectors.toList());
        List<String> primaryKey ;
        if(parsedPrimaryKeys.isEmpty()){
            primaryKey = columns.stream().filter(c->c.isPartOfUniqueIndex()).collect(Collectors.toList())
                    .stream().map(d -> d.getName()).collect(Collectors.toList());
        }else{
            primaryKey = parsedPrimaryKeys.stream().map(d->d.getName()).collect(Collectors.toList());
        }

    return new SnowflakeTable(
        table.getName(),
        table.getName(),
        table.getRemarks(),
        primaryKey,
        null,
        null,
        null,
        new FileFormat(table.getName(), "PARQUET"),
        mapSchemaCrawlerColumnDefinition(columns));
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

    public static List<TableDefinition> mapAWSGlueTables(List<Table> glueTables, List<UserDefinedTable> userDefinedTables){
        List<SnowflakeTable> glueTableList = glueTables.stream().map(table -> mapAWSGlueTable(table)).collect(Collectors.toList());
    return userTableDefinitions(glueTableList, userDefinedTables).stream()
        .map(userTable -> (TableDefinition) userTable)
        .collect(Collectors.toList());
    }

    private static SnowflakeTable mapAWSGlueTable(Table table) {
    return new SnowflakeTable(
        table.getName(),
        table.getName(),
        table.getRemarks(),
        new ArrayList<>(),
        null,
        null,
        null,
        GlueCrawler.getTableFileFormatMap().get(table),
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
}
