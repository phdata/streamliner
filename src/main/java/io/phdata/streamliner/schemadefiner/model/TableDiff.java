package io.phdata.streamliner.schemadefiner.model;

import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class TableDiff {
  private String type;
  public String destinationName;
  public Boolean existsInDestination;
  public Boolean existsInSource;
  private List<ColumnDiff> columnDiffs;

  public TableDiff() {}

  public TableDiff(
      String type,
      String destinationName,
      List<ColumnDiff> columnDiffs,
      Boolean existsInDestination,
      Boolean existsInSource) {
    this.type = type;
    this.destinationName = destinationName;
    this.columnDiffs = columnDiffs;
    this.existsInDestination = existsInDestination;
    this.existsInSource = existsInSource;
  }

  public boolean allChangesAreCompatible(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);

    for (ColumnDiff colDiff : columnDiffs) {
      if (colDiff.getIsDeleted()) {
        return false;
      } else if (colDiff.getIsUpdate()) {
        ColumnDefinition currDef = colDiff.getCurrentColumnDef();
        ColumnDefinition prevDef = colDiff.getPreviousColumnDef();
        if (!currDef.getDataType().equalsIgnoreCase(prevDef.getDataType())) {
          return false;
        } else if (!isSnowflakeStringDataType(javaTypeMap, currDef)
            || currDef.getPrecision() == prevDef.getPrecision()) {
          /* currently datatype increase is implemented only for snowflake string data type
          * ColumnDiff isUpdate becomes true if any variable of the ColumnDefinition changes.
          * Since we are checking increment of column length only, precision is being checked here. */
          return false;
        }
      }
    }
    return true;
  }

  private boolean isSnowflakeStringDataType(
      Map<String, Map<String, String>> javaTypeMap, ColumnDefinition currDef) {
    return StreamlinerUtil.snowflakeStringDataType.contains(
        currDef.mapDataType(currDef.getDataType(), javaTypeMap, "SNOWFLAKE").toUpperCase());
  }

  public boolean isColumnAdded(){
    return columnDiffs.stream().anyMatch(c -> c.getIsAdd());
  }

  public boolean isColumnSizeChanged(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);
    return columnDiffs.stream()
        .anyMatch(c -> isColumnSizeChanged(c.getCurrentColumnDef(), c.getPreviousColumnDef(), javaTypeMap));
  }

  private boolean isColumnSizeChanged(
      ColumnDefinition currDef,
      ColumnDefinition prevDef,
      Map<String, Map<String, String>> typeMapping) {
    if (currDef == null || prevDef == null) {
      return false;
    } else if (isSnowflakeStringDataType(typeMapping, currDef)
        && currDef.getPrecision() > prevDef.getPrecision()
        && currDef.getPrecision() <= ColumnDefinition.SNOWFLAKE_MAX_LENGTH) {
      return true;
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
                        column.getCurrentColumnDef().getComment()))
            .collect(Collectors.toList());
    return StringUtils.join(list, ",\n");
  }

  public String alterColumnDDL(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);

    List<String> list =
        columnDiffs.stream()
            .filter(
                c ->
                    isColumnSizeChanged(
                        c.getCurrentColumnDef(), c.getPreviousColumnDef(), javaTypeMap))
            .map(
                col ->
                    String.format(
                        "COLUMN %s SET DATA TYPE %s",
                        col.getCurrentColumnDef().getDestinationName(),
                        col.getCurrentColumnDef().mapDataTypeSnowflake(javaTypeMap)))
            .collect(Collectors.toList());
    return StringUtils.join(list, ",\n");
  }
}
