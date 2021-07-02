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
public class ConfigurationDiff{
  private String name;
  private String environment;
  private String pipeline;
  private Destination previousDestination;
  public Destination currentDestination;
  private List<TableDiff> tableDiffs;

  public ConfigurationDiff() {}

  public ConfigurationDiff(
      String name,
      String environment,
      String pipeline,
      Destination previousDestination,
      Destination currentDestination,
      List<TableDiff> tableDiffs) {
    this.name = name;
    this.environment = environment;
    this.pipeline = pipeline;
    this.previousDestination = previousDestination;
    this.currentDestination = currentDestination;
    this.tableDiffs = tableDiffs;
  }
}
