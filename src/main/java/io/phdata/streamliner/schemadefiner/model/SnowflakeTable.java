package io.phdata.streamliner.schemadefiner.model;

import java.util.List;
import java.util.Map;

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

  @Override
  public String getType() {
    return type;
  }

  @Override
  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String getSourceName() {
    return sourceName;
  }

  @Override
  public void setSourceName(String sourceName) {
    this.sourceName = sourceName;
  }

  @Override
  public String getDestinationName() {
    return destinationName;
  }

  @Override
  public void setDestinationName(String destinationName) {
    this.destinationName = destinationName;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Override
  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  @Override
  public void setPrimaryKeys(List<String> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public String getChangeColumn() {
    return changeColumn;
  }

  public void setChangeColumn(String changeColumn) {
    this.changeColumn = changeColumn;
  }

  public String getIncrementalTimeStamp() {
    return incrementalTimeStamp;
  }

  public void setIncrementalTimeStamp(String incrementalTimeStamp) {
    this.incrementalTimeStamp = incrementalTimeStamp;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  @Override
  public List<ColumnDefinition> getColumns() {
    return columns;
  }

  @Override
  public void setColumns(List<ColumnDefinition> columns) {
    this.columns = columns;
  }
}
