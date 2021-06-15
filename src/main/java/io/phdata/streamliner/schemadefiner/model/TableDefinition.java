package io.phdata.streamliner.schemadefiner.model;

import java.util.List;
import java.util.Map;

public class TableDefinition {
    private String type;
    private String sourceName;
    private String destinationName;
    private String comment;
    private List<String> primaryKeys;
    private List<ColumnDefinition> columns;
    private FileFormat fileFormat;
    private String checkColumn;
    private Map<String,String> metadata;
    private Integer numberOfMappers;
    private String splitByColumn;
    private Integer numberOfPartitions;
    private String changeColumn;
    private String incrementalTimeStamp;

    public TableDefinition() {
    }

    // Snowflake Table
    public TableDefinition(String type, String sourceName, String destinationName, String comment, List<String> primaryKeys, String changeColumn, String incrementalTimeStamp, Map<String, String> metadata, FileFormat fileFormat,List<ColumnDefinition> columns) {
        this.type = type;
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.comment = comment;
        this.primaryKeys = primaryKeys;
        this.columns = columns;
        this.fileFormat = fileFormat;
        this.changeColumn = changeColumn;
        this.incrementalTimeStamp = incrementalTimeStamp;
        this.metadata = metadata;
    }

    // Hadoop Table
    public TableDefinition(String type, String sourceName, String destinationName, String checkColumn, String comment, List<String> primaryKeys, Map<String,String> metadata, Integer numberOfMappers, String splitByColumn, Integer numberOfPartitions, List<ColumnDefinition> columns) {
        this.type = type;
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.checkColumn = checkColumn;
        this.comment = comment;
        this.primaryKeys = primaryKeys;
        this.metadata = metadata;
        this.numberOfMappers = numberOfMappers;
        this.splitByColumn = splitByColumn;
        this.numberOfPartitions = numberOfPartitions;
        this.columns = columns;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public FileFormat getFileFormat() {
        return fileFormat;
    }

    public void setFileFormat(FileFormat fileFormat) {
        this.fileFormat = fileFormat;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnDefinition> columns) {
        this.columns = columns;
    }

    public String getCheckColumn() {
        return checkColumn;
    }

    public void setCheckColumn(String checkColumn) {
        this.checkColumn = checkColumn;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
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
}
