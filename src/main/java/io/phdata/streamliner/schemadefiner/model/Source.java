package io.phdata.streamliner.schemadefiner.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Jdbc.class, name = "Jdbc"),
        @JsonSubTypes.Type(value = GlueCatalog.class, name = "Glue") })
@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class Source {
    private String type;

    public Source() {
    }
}
