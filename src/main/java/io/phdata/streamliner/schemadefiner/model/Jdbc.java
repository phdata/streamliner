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
  public String driverClass = "";
  public String url;
  public String username;
  public String passwordFile;
  private String jceKeyStorePath;
  private String keystoreAlias;
  public String schema;
  private List<String> tableTypes;
  // override to the metadata
  private List<UserDefinedTable> userDefinedTable;
  private Map<String, String> metadata;
  // for table whitelisting
  private List<String> tables;
  //used to fetch snowflake tables details in batch of value provided
  private int batchTableCount;
  /* snowflake schema crawler uses two approach to extract schemas.
  *  use "information_schema" for INFORMATION SCHEMA  and "show_command" for SHOW COMMAND approach.
  * */
  private String snowflakeCrawler;

  public Jdbc() {}

  public Jdbc(String url, String username, String schema, List<String> tableTypes) {
    this.url = url;
    this.username = username;
    this.schema = schema;
    this.tableTypes = tableTypes;
  }
}
