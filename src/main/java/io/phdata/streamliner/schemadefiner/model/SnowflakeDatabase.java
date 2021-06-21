package io.phdata.streamliner.schemadefiner.model;

public class SnowflakeDatabase {
  private String name;
  private String schema;

  public SnowflakeDatabase() {}

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }
}
