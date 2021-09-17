package io.phdata.streamliner.schemadefiner.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.*;

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
  // used to fetch snowflake tables details in batch of value provided
  private int batchTableCount;
  /* This is used to manage allowed schema changes.
  For example: To exclude column add for CSV users we should not pass "COLUMN_ADD" in the list. Then column add becomes incompatible change for CSV users.
  If this parameter is not provided in config, by default it includes all the values from enum SchemaChanges. This is handled in code.
  */
  private Set<SchemaChanges> validSchemaChanges = new HashSet<>();
  /* Used to ignore the tables.
  *  In case tables(used for table whitelisting) and ignoreTables are used simultaneously then tables are ignored from whitelisted tables.
  * */
  private Set<String> ignoreTables;

  public Jdbc() {}

  public Jdbc(String url, String username, String schema, List<String> tableTypes) {
    this.url = url;
    this.username = username;
    this.schema = schema;
    this.tableTypes = tableTypes;
  }
}
