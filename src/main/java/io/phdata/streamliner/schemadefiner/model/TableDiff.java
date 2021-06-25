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
public class TableDiff {
  private String type;
  private String destinationName;
  private Boolean existsInDestination;
  private Boolean existsInSource;
  private List<ColumnDiff> columnDiffs;

  public TableDiff() {}

  public TableDiff(
      String type,
      String destinationName,
      List<ColumnDiff> columnDiffs,
      Boolean existsInDestination,
      Boolean existsInSource) {
    this.type = type;
    this.destinationName = destinationName;
    this.columnDiffs = columnDiffs;
    this.existsInDestination = existsInDestination;
    this.existsInSource = existsInSource;
  }
}
