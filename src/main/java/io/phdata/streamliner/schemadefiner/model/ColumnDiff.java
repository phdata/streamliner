package io.phdata.streamliner.schemadefiner.model;

public class ColumnDiff {
  private String previousName;
  private String newName;
  private String previousDataType;
  private String newDataType;
  private String previousComment;
  private String newComment;
  private Integer previousPrecision;
  private Integer newPrecision;
  private Integer previousScale;
  private Integer newScale;
  private Boolean isDeleted;
  private Boolean isAdd;
  private Boolean isUpdate;

  public ColumnDiff() {}

  public ColumnDiff(
      String previousName,
      String newName,
      String previousDataType,
      String newDataType,
      String previousComment,
      String newComment,
      Integer previousPrecision,
      Integer newPrecision,
      Integer previousScale,
      Integer newScale,
      Boolean isDeleted,
      Boolean isAdd,
      Boolean isUpdate) {
    this.previousName = previousName;
    this.newName = newName;
    this.previousDataType = previousDataType;
    this.newDataType = newDataType;
    this.previousComment = previousComment;
    this.newComment = newComment;
    this.previousPrecision = previousPrecision;
    this.newPrecision = newPrecision;
    this.previousScale = previousScale;
    this.newScale = newScale;
    this.isDeleted = isDeleted;
    this.isAdd = isAdd;
    this.isUpdate = isUpdate;
  }

  public String getPreviousName() {
    return previousName;
  }

  public void setPreviousName(String previousName) {
    this.previousName = previousName;
  }

  public String getNewName() {
    return newName;
  }

  public void setNewName(String newName) {
    this.newName = newName;
  }

  public String getPreviousDataType() {
    return previousDataType;
  }

  public void setPreviousDataType(String previousDataType) {
    this.previousDataType = previousDataType;
  }

  public String getNewDataType() {
    return newDataType;
  }

  public void setNewDataType(String newDataType) {
    this.newDataType = newDataType;
  }

  public String getPreviousComment() {
    return previousComment;
  }

  public void setPreviousComment(String previousComment) {
    this.previousComment = previousComment;
  }

  public String getNewComment() {
    return newComment;
  }

  public void setNewComment(String newComment) {
    this.newComment = newComment;
  }

  public Integer getPreviousPrecision() {
    return previousPrecision;
  }

  public void setPreviousPrecision(Integer previousPrecision) {
    this.previousPrecision = previousPrecision;
  }

  public Integer getNewPrecision() {
    return newPrecision;
  }

  public void setNewPrecision(Integer newPrecision) {
    this.newPrecision = newPrecision;
  }

  public Integer getPreviousScale() {
    return previousScale;
  }

  public void setPreviousScale(Integer previousScale) {
    this.previousScale = previousScale;
  }

  public Integer getNewScale() {
    return newScale;
  }

  public void setNewScale(Integer newScale) {
    this.newScale = newScale;
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
