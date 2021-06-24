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

  public Jdbc(String url, String username, String schema, List<String> tableTypes) {
    this.url = url;
    this.username = username;
    this.schema = schema;
    this.tableTypes = tableTypes;
  }
}
