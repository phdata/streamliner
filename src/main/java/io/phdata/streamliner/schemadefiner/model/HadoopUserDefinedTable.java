package io.phdata.streamliner.schemadefiner.model;

import java.util.List;
import java.util.Map;

public class HadoopUserDefinedTable extends UserDefinedTable {
  private String type;
  private String name;
  private List<String> primaryKeys;
  private String checkColumn;
  private Integer numberOfMappers;
  private String splitByColumn;
  private Integer numberOfPartitions;
  private Map<String, String> metadata;

  public HadoopUserDefinedTable() {}

  @Override
  public String getType() {
    return type;
  }

  @Override
  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  @Override
  public void setPrimaryKeys(List<String> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public String getCheckColumn() {
    return checkColumn;
  }

  public void setCheckColumn(String checkColumn) {
    this.checkColumn = checkColumn;
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

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }
}
