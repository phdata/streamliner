package io.phdata.streamliner.schemadefiner.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Jdbc.class, name = "Jdbc"),
        @JsonSubTypes.Type(value = GlueCatalog.class, name = "Glue") })
public class Source {
    private String type;

    public Source() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
