package io.phdata.streamliner.schemadefiner.model;

import java.util.List;

public class ConfigurationDiff{
  private String name;
  private String environment;
  private String pipeline;
  private Destination previousDestination;
  private Destination currentDestination;
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

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getEnvironment() {
    return environment;
  }

  public void setEnvironment(String environment) {
    this.environment = environment;
  }

  public String getPipeline() {
    return pipeline;
  }

  public void setPipeline(String pipeline) {
    this.pipeline = pipeline;
  }

  public Destination getPreviousDestination() {
    return previousDestination;
  }

  public void setPreviousDestination(Destination previousDestination) {
    this.previousDestination = previousDestination;
  }

  public Destination getCurrentDestination() {
    return currentDestination;
  }

  public void setCurrentDestination(Destination currentDestination) {
    this.currentDestination = currentDestination;
  }

  public List<TableDiff> getTableDiffs() {
    return tableDiffs;
  }

  public void setTableDiffs(List<TableDiff> tableDiffs) {
    this.tableDiffs = tableDiffs;
  }
}
