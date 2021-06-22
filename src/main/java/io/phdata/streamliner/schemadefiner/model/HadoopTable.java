package io.phdata.streamliner.schemadefiner.model;

import java.util.List;
import java.util.Map;

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

  public String getCheckColumn() {
    return checkColumn;
  }

  public void setCheckColumn(String checkColumn) {
    this.checkColumn = checkColumn;
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

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public Integer getNumberOfMappers() {
    return numberOfMappers;
  }

  public void setNumberOfMappers(Integer numberOfMappers) {
    this.numberOfMappers = numberOfMappers;
  }

  public String getSplitByColumn() {
    return splitByColumn;
  }

  public void setSplitByColumn(String splitByColumn) {
    this.splitByColumn = splitByColumn;
  }

  public Integer getNumberOfPartitions() {
    return numberOfPartitions;
  }

  public void setNumberOfPartitions(Integer numberOfPartitions) {
    this.numberOfPartitions = numberOfPartitions;
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
