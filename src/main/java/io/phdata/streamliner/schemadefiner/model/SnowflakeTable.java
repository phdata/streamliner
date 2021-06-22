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
public class SnowflakeTable extends TableDefinition {
  private String type;
  private String sourceName;
  private String destinationName;
  private String comment;
  private List<String> primaryKeys;
  private String changeColumn;
  private String incrementalTimeStamp;
  private Map<String, String> metadata;
  private FileFormat fileFormat;
  private List<ColumnDefinition> columns;

  public SnowflakeTable() {}

  public SnowflakeTable(
      String sourceName,
      String destinationName,
      String comment,
      List<String> primaryKeys,
      String changeColumn,
      String incrementalTimeStamp,
      Map<String, String> metadata,
      FileFormat fileFormat,
      List<ColumnDefinition> columns) {
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.comment = comment;
    this.primaryKeys = primaryKeys;
    this.changeColumn = changeColumn;
    this.incrementalTimeStamp = incrementalTimeStamp;
    this.metadata = metadata;
    this.fileFormat = fileFormat;
    this.columns = columns;
  }
}
