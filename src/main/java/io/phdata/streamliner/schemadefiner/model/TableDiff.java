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

  public boolean allChangesAreCompatible(){
    return columnDiffs.stream().allMatch(c -> c.getIsAdd());
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
}
