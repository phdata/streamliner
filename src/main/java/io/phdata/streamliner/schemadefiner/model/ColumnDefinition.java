package io.phdata.streamliner.schemadefiner.model;

public class ColumnDefinition {
    private String sourceName;
    private String destinationName;
    private String dataType;
    private String comment;
    private Integer precision;
    private Integer scale;

    public ColumnDefinition() {
    }

    public ColumnDefinition(String sourceName, String destinationName, String dataType, String comment, Integer precision, Integer scale) {
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.dataType = dataType;
        this.comment = comment;
        this.precision = precision;
        this.scale = scale;
    }

    public ColumnDefinition(String sourceName, String destinationName, String dataType, String comment) {
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.dataType = dataType;
        this.comment = comment;
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

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }
}
