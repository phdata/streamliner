package io.phdata.streamliner.schemadefiner.model;

import java.util.List;
import java.util.Map;

public class Jdbc extends Source {
  private String type;
  private String driverClass;
  private String url;
  private String username;
  private String passwordFile;
  private String jceKeyStorePath;
  private String keystoreAlias;
  private String schema;
  private List<String> tableTypes;
  private List<UserDefinedTable> userDefinedTable;
  private Map<String, String> metadata;

  public Jdbc() {}

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getDriverClass() {
    return driverClass;
  }

  public void setDriverClass(String driverClass) {
    this.driverClass = driverClass;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPasswordFile() {
    return passwordFile;
  }

  public void setPasswordFile(String passwordFile) {
    this.passwordFile = passwordFile;
  }

  public String getJceKeyStorePath() {
    return jceKeyStorePath;
  }

  public void setJceKeyStorePath(String jceKeyStorePath) {
    this.jceKeyStorePath = jceKeyStorePath;
  }

  public String getKeystoreAlias() {
    return keystoreAlias;
  }

  public void setKeystoreAlias(String keystoreAlias) {
    this.keystoreAlias = keystoreAlias;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public List<String> getTableTypes() {
    return tableTypes;
  }

  public void setTableTypes(List<String> tableTypes) {
    this.tableTypes = tableTypes;
  }

  public List<UserDefinedTable> getUserDefinedTable() {
    return userDefinedTable;
  }

  public void setUserDefinedTable(List<UserDefinedTable> userDefinedTable) {
    this.userDefinedTable = userDefinedTable;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }
}
