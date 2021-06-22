package io.phdata.streamliner.schemadefiner.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class ColumnDiff {
  private ColumnDefinition previousColumnDef;
  private ColumnDefinition currentColumnDef;
  private Boolean isDeleted;
  private Boolean isAdd;
  private Boolean isUpdate;

  public ColumnDiff() {}

  public ColumnDiff(
      ColumnDefinition previousColumnDefinition,
      ColumnDefinition currentColumnDefinition,
      Boolean isDeleted,
      Boolean isAdd,
      Boolean isUpdate) {
    this.previousColumnDef = previousColumnDefinition;
    this.currentColumnDef = currentColumnDefinition;
    this.isDeleted = isDeleted;
    this.isAdd = isAdd;
    this.isUpdate = isUpdate;
  }
}
