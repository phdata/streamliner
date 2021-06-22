package io.phdata.streamliner.schemadefiner.model;

import java.util.List;

public class GlueCatalog extends Source {
  private String type;
  private String region;
  private String database;
  private List<UserDefinedTable> userDefinedTable;

  public GlueCatalog() {}

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public List<UserDefinedTable> getUserDefinedTable() {
    return userDefinedTable;
  }

  public void setUserDefinedTable(List<UserDefinedTable> userDefinedTable) {
    this.userDefinedTable = userDefinedTable;
  }
}
