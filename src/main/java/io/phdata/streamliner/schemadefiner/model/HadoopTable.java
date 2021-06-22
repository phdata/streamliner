package io.phdata.streamliner.schemadefiner.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class HadoopTable extends TableDefinition {
  private String type;
  private String sourceName;
  private String destinationName;
  private String checkColumn;
  private String comment;
  private List<String> primaryKeys;
  private Map<String, String> metadata;
  private Integer numberOfMappers;
  private String splitByColumn;
  private Integer numberOfPartitions;
  private List<ColumnDefinition> columns;

  public HadoopTable() {}

  public HadoopTable(
      String sourceName,
      String destinationName,
      String checkColumn,
      String comment,
      List<String> primaryKeys,
      Map<String, String> metadata,
      Integer numberOfMappers,
      String splitByColumn,
      Integer numberOfPartitions,
      List<ColumnDefinition> columns) {
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.checkColumn = checkColumn;
    this.comment = comment;
    this.primaryKeys = primaryKeys;
    this.metadata = metadata;
    this.numberOfMappers = numberOfMappers;
    this.splitByColumn = splitByColumn;
    this.numberOfPartitions = numberOfPartitions;
    this.columns = columns;
  }
}
