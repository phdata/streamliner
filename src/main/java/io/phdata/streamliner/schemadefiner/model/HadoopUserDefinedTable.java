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
}
