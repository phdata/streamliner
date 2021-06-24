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
public class Configuration {
    private String name;
    private String environment;
    private String pipeline;
    private Source source;
    private Destination destination;
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
}

