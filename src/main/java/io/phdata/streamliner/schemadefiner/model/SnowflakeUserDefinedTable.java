package io.phdata.streamliner.schemadefiner.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class SnowflakeUserDefinedTable extends UserDefinedTable {

  private String type;
  private String name;
  private List<String> primaryKeys;
  private FileFormat fileFormat;

  public SnowflakeUserDefinedTable() {}
}
