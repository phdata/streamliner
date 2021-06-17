package io.phdata.streamliner.schemadefiner.model;

import java.util.List;

public class TableDiff {
  private String type;
  private String destinationName;
  private Boolean existsInDestination;
  private List<ColumnDiff> columnDiffs;

  public TableDiff() {}

  public TableDiff(
      String type,
      String destinationName,
      List<ColumnDiff> columnDiffs,
      Boolean existsInDestination) {
    this.type = type;
    this.destinationName = destinationName;
    this.columnDiffs = columnDiffs;
    this.existsInDestination = existsInDestination;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getDestinationName() {
    return destinationName;
  }

  public void setDestinationName(String destinationName) {
    this.destinationName = destinationName;
  }

  public List<ColumnDiff> getColumnDiffs() {
    return columnDiffs;
  }

  public void setColumnDiffs(List<ColumnDiff> columnDiffs) {
    this.columnDiffs = columnDiffs;
  }

  public Boolean getExistsInDestination() {
    return existsInDestination;
  }

  public void setExistsInDestination(Boolean existsInDestination) {
    this.existsInDestination = existsInDestination;
  }
}
