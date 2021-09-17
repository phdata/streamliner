package io.phdata.streamliner.schemadefiner.model;

import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.schemadefiner.util.TemplateUtil;
import io.phdata.streamliner.util.JavaHelper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class TableDiff {
  private String type;
  public String destinationName;
  public boolean existsInDestination;
  public boolean existsInSource;
  private List<ColumnDiff> columnDiffs;

  public TableDiff() {}

  public TableDiff(
      String type,
      String destinationName,
      List<ColumnDiff> columnDiffs,
      boolean existsInDestination,
      boolean existsInSource) {
    this.type = type;
    this.destinationName = destinationName;
    this.columnDiffs = columnDiffs;
    this.existsInDestination = existsInDestination;
    this.existsInSource = existsInSource;
  }

  public boolean allChangesAreCompatible(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping,
      Set<SchemaChanges> validSchemaChanges) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);
    if (validSchemaChanges == null || validSchemaChanges.isEmpty()) {
      validSchemaChanges.addAll(Arrays.asList(SchemaChanges.values()));
    }

    if (isNewTable()) {
      if (!validSchemaChanges.contains(SchemaChanges.TABLE_ADD)) {
        return false;
      } else {
        return true;
      }
    }

    for (ColumnDiff colDiff : columnDiffs) {
      if (colDiff.getIsDeleted()) {
        return false;
      }
      if (colDiff.getIsAdd() && !validSchemaChanges.contains(SchemaChanges.COLUMN_ADD)) {
        return false;
      }
      if (colDiff.getIsUpdate()) {
        ColumnDefinition currDef = colDiff.getCurrentColumnDef();
        ColumnDefinition prevDef = colDiff.getPreviousColumnDef();
        if (!isDataTypeSame(currDef, prevDef)) {
          return false;
        }
        if (!isColumnNullableSame(currDef, prevDef)
            && !validSchemaChanges.contains(SchemaChanges.UPDATE_COLUMN_NULLABILITY)) {
          return false;
        }
        if (!isColumnCommentSame(currDef, prevDef)
            && !validSchemaChanges.contains(SchemaChanges.UPDATE_COLUMN_COMMENT)) {
          return false;
        }
        if (isPrecisionChangeValid(currDef, prevDef, javaTypeMap)
            && !validSchemaChanges.contains(SchemaChanges.EXTEND_COLUMN_LENGTH)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean isNewTable() {
    return existsInSource && !existsInDestination;
  }

  public boolean isDataTypeSame(ColumnDefinition currDef, ColumnDefinition prevDef) {
    return currDef.getDataType().equalsIgnoreCase(prevDef.getDataType());
  }

  public boolean isColumnNullableSame(ColumnDefinition currDef, ColumnDefinition prevDef) {
    return currDef.isNullable() == prevDef.isNullable();
  }

  private boolean isSnowflakeStringDataType(
      Map<String, Map<String, String>> javaTypeMap, ColumnDefinition currDef) {
    return StreamlinerUtil.snowflakeStringDataType.contains(
        currDef.mapDataType(currDef.getDataType(), javaTypeMap, "SNOWFLAKE").toUpperCase());
  }

  public boolean isColumnAdded(){
    return columnDiffs.stream().anyMatch(c -> c.getIsAdd());
  }

  public boolean areThereAnyChanges(
          scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
                  typeMapping) {
    return isColumnModified(typeMapping) || isColumnAdded();
  }

  public boolean isColumnModified(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);
    return columnDiffs.stream()
        .anyMatch(c -> isColumnModified(c.getCurrentColumnDef(), c.getPreviousColumnDef(), javaTypeMap));
  }

  private boolean isColumnModified(
      ColumnDefinition currDef,
      ColumnDefinition prevDef,
      Map<String, Map<String, String>> typeMapping) {
    if (currDef == null || prevDef == null) {
      return false;
    } else if (isPrecisionChangeValid(currDef, prevDef, typeMapping)
        || (!isColumnCommentSame(currDef, prevDef) || !isColumnNullableSame(currDef, prevDef))) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isColumnCommentSame(ColumnDefinition currDef, ColumnDefinition prevDef) {
    return currDef.getComment().equals(prevDef.getComment());
  }

  public boolean isPrecisionChangeValid(
      ColumnDefinition currDef,
      ColumnDefinition prevDef,
      Map<String, Map<String, String>> typeMapping) {
    if (currDef.getPrecision() > prevDef.getPrecision()
        && currDef.getPrecision() <= ColumnDefinition.SNOWFLAKE_MAX_LENGTH) {
      return isSnowflakeStringDataType(typeMapping, currDef);
    } else {
      return false;
    }
  }

  public String columnDDL(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);

    List<String> list =
        columnDiffs.stream()
            .filter(colDiff -> colDiff.getIsAdd())
            .map(
                columnDiff ->
                    String.format(
                        "%s %s",
                        StreamlinerUtil.quoteIdentifierIfNeeded(
                            columnDiff.getCurrentColumnDef().getDestinationName()),
                        columnDiff.getCurrentColumnDef().mapDataTypeSnowflake(javaTypeMap)))
            .collect(Collectors.toList());
    return StringUtils.join(list, ",\n");
  }

  public String createTableColumnDDL(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);

    List<String> list =
        columnDiffs.stream()
            .map(
                column ->
                    String.format(
                        "%s %s COMMENT '%s'",
                        StreamlinerUtil.quoteIdentifierIfNeeded(
                            column.getCurrentColumnDef().getDestinationName()),
                        column.getCurrentColumnDef().mapDataTypeSnowflake(javaTypeMap),
                        TemplateUtil.snowflakeEscape(column.getCurrentColumnDef().getComment())))
            .collect(Collectors.toList());
    return StringUtils.join(list, ",\n");
  }

  public String alterColumnDDL(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);
    List<String> alterColumnDDL = new ArrayList<>();

    columnDiffs.stream()
        .filter(
            c -> isColumnModified(c.getCurrentColumnDef(), c.getPreviousColumnDef(), javaTypeMap))
        .forEach(
            col -> {
              ColumnDefinition currDef = col.getCurrentColumnDef();
              ColumnDefinition prevDef = col.getPreviousColumnDef();
              if (isPrecisionChangeValid(currDef, prevDef, javaTypeMap)) {
                alterColumnDDL.add(
                    String.format(
                        "COLUMN %s SET DATA TYPE %s",
                        StreamlinerUtil.quoteIdentifierIfNeeded(currDef.getDestinationName()), currDef.mapDataTypeSnowflake(javaTypeMap)));
              }
              if (!isColumnCommentSame(currDef, prevDef)) {
                alterColumnDDL.add(
                    String.format(
                        "COLUMN %s COMMENT '%s'",
                        StreamlinerUtil.quoteIdentifierIfNeeded(currDef.getDestinationName()), TemplateUtil.snowflakeEscape(currDef.getComment())));
              }
              if (!isColumnNullableSame(currDef, prevDef)) {
                if (currDef.isNullable() == true) {
                  alterColumnDDL.add(
                      String.format("COLUMN %s DROP NOT NULL", StreamlinerUtil.quoteIdentifierIfNeeded(currDef.getDestinationName())));
                } else {
                  alterColumnDDL.add(
                      String.format("COLUMN %s SET NOT NULL", StreamlinerUtil.quoteIdentifierIfNeeded(currDef.getDestinationName())));
                }
              }
            });
    return StringUtils.join(alterColumnDDL, ",\n");
  }
}
