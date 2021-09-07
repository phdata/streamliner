package io.phdata.streamliner.schemadefiner.model;

public enum SchemaChanges {
  /* Currently schema evolution feature supported by Streamliner 5.x */
  TABLE_ADD("table-add"),
  COLUMN_ADD("column-add"),
  UPDATE_COLUMN_COMMENT("column-comment"),
  UPDATE_COLUMN_NULLABILITY("column-nullability"),
  EXTEND_COLUMN_LENGTH("extend-column-length");

  String value;

  SchemaChanges(String str) {
    this.value = str;
  }

  public String value() {
    return this.value;
  }
}
