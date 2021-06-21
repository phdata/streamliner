package io.phdata.streamliner.schemadefiner.model;

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

  public ColumnDefinition getPreviousColumnDef() {
    return previousColumnDef;
  }

  public void setPreviousColumnDef(ColumnDefinition previousColumnDef) {
    this.previousColumnDef = previousColumnDef;
  }

  public ColumnDefinition getCurrentColumnDef() {
    return currentColumnDef;
  }

  public void setCurrentColumnDef(ColumnDefinition currentColumnDef) {
    this.currentColumnDef = currentColumnDef;
  }

  public Boolean getDeleted() {
    return isDeleted;
  }

  public void setDeleted(Boolean deleted) {
    isDeleted = deleted;
  }

  public Boolean getAdd() {
    return isAdd;
  }

  public void setAdd(Boolean add) {
    isAdd = add;
  }

  public Boolean getUpdate() {
    return isUpdate;
  }

  public void setUpdate(Boolean update) {
    isUpdate = update;
  }
}
