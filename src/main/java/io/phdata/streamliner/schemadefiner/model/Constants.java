package io.phdata.streamliner.schemadefiner.model;

public enum Constants {
  STREAMLINER_DIFF_FILE("streamliner-diff.yml"),
  COLUMNS_DELETED("columns-deleted"),
  COLUMNS_ADDED("columns-added"),
  COLUMNS_UPDATED("columns-updated"),
  TABLES_ADDED("tables-added"),
  TABLES_DELETED("tables-deleted"),
  TABLES_INCOMPATIBLE("incompatible-tables");
  String value;

  Constants(String str) {
    this.value = str;
  }

  public String value() {
    return this.value;
  }
}
