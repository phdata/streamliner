package io.phdata.streamliner.schemadefiner.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
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
}
