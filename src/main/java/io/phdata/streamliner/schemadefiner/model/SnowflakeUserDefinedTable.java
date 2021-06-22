package io.phdata.streamliner.schemadefiner.model;

import java.util.List;

public class SnowflakeUserDefinedTable extends UserDefinedTable {

  private String type;
  private String name;
  private List<String> primaryKeys;
  private FileFormat fileFormat;

  public SnowflakeUserDefinedTable() {}

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

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }
}
