package io.phdata.streamliner.schemadefiner.model;

import java.util.List;
import java.util.Map;

public class UserDefinedTable {

    private String type;
    private String name;
    private List<String> primaryKeys;
    private FileFormat fileFormat;
    private String checkColumn;
    private Integer numberOfMappers;
    private String splitByColumn;
    private Integer numberOfPartitions;
    private Map<String,String> metadata;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public FileFormat getFileFormat() {
        return fileFormat;
    }

    public void setFileFormat(FileFormat fileFormat) {
        this.fileFormat = fileFormat;
    }

    public String getCheckColumn() {
        return checkColumn;
    }

    public void setCheckColumn(String checkColumn) {
        this.checkColumn = checkColumn;
    }

    public Integer getNumberOfMappers() {
        return numberOfMappers;
    }

    public void setNumberOfMappers(Integer numberOfMappers) {
        this.numberOfMappers = numberOfMappers;
    }

    public String getSplitByColumn() {
        return splitByColumn;
    }

    public void setSplitByColumn(String splitByColumn) {
        this.splitByColumn = splitByColumn;
    }

    public Integer getNumberOfPartitions() {
        return numberOfPartitions;
    }

    public void setNumberOfPartitions(Integer numberOfPartitions) {
        this.numberOfPartitions = numberOfPartitions;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
}
