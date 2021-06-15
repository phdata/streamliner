package io.phdata.streamliner.schemadefiner.model;

public class IngestConfigFileFormat {

    private String name;
    private Options options;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Options getOptions() {
        return options;
    }

    public void setOptions(Options options) {
        this.options = options;
    }
}
