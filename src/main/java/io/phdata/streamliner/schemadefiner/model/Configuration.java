package io.phdata.streamliner.schemadefiner.model;


import java.util.List;

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getPipeline() {
        return pipeline;
    }

    public void setPipeline(String pipeline) {
        this.pipeline = pipeline;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public List<TableDefinition> getTables() {
        return tables;
    }

    public void setTables(List<TableDefinition> tables) {
        this.tables = tables;
    }
}

