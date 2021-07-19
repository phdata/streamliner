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
public class GlueCatalog extends Source {
  private String type;
  private String region;
  private String database;
  // override to the metadata
  private List<UserDefinedTable> userDefinedTable;
  // for table whitelisting
  private List<String> tables;

  public GlueCatalog() {}
}
