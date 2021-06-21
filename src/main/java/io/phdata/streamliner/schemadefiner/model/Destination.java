package io.phdata.streamliner.schemadefiner.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Hadoop.class, name = "Hadoop"),
        @JsonSubTypes.Type(value = Snowflake.class, name = "Snowflake") })
public class Destination {
    private String type;

    public Destination() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
