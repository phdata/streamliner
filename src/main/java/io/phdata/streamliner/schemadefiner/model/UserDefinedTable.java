package io.phdata.streamliner.schemadefiner.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = HadoopUserDefinedTable.class, name = "Hadoop"),
        @JsonSubTypes.Type(value = SnowflakeUserDefinedTable.class, name = "Snowflake") })
@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class UserDefinedTable {

    private String type;
    private String name;
    private List<String> primaryKeys;

    public UserDefinedTable() {
    }
}
