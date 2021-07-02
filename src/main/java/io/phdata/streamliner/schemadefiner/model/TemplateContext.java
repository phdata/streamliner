package io.phdata.streamliner.schemadefiner.model;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class TemplateContext {

  @Getter private final List<String> errors = new ArrayList<>();

  public void addError(String msg) {
    errors.add(msg);
  }

  public boolean hasErrors() {
    return !errors.isEmpty();
  }
}
