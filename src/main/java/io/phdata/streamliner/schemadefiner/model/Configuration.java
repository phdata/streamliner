package io.phdata.streamliner.schemadefiner.model;


import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class Configuration {
    public String name;
    public String environment;
    private String pipeline;
    public Source source;
    public Destination destination;
    private List<TableDefinition> tables;

    public Configuration() {
    }

    public Configuration(String name, String environment, String pipeline, Source source, Destination destination, List<TableDefinition> tables) {
        this.name = name;
        this.environment = environment;
        this.pipeline = pipeline;
        this.source = source;
        this.destination = destination;
        this.tables = tables;
    }

    public Configuration(List<TableDefinition> tables){
        this.tables = tables;
    }

    public Configuration(String name, String environment, String pipeline, Source source, Destination destination) {
        this.name = name;
        this.environment = environment;
        this.pipeline = pipeline;
        this.source = source;
        this.destination = destination;
    }

  public void addTableDefinition(TableDefinition table) {
    if (this.tables == null) {
      this.tables = new ArrayList<>();
      this.tables.add(table);
    } else {
      this.tables.add(table);
    }
  }
}

