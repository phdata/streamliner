package io.phdata.streamliner.schemadefiner.model;

public enum Constants {
  STREAMLINER_DIFF_FILE("streamliner-diff.yml");
  String value;

  Constants(String str) {
    this.value = str;
  }

  public String value() {
    return this.value;
  }
}
