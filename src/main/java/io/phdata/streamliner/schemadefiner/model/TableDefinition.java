package io.phdata.streamliner.schemadefiner.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = HadoopTable.class, name = "Hadoop"),
        @JsonSubTypes.Type(value = SnowflakeTable.class, name = "Snowflake") })
@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class TableDefinition {
    private String type;
    private String sourceName;
    public String destinationName;
    private List<String> primaryKeys;
    private List<ColumnDefinition> columns;

    public TableDefinition() {
    }

  public TableDefinition(
      String type,
      String sourceName,
      String destinationName,
      List<String> primaryKeys,
      List<ColumnDefinition> columns) {
    this.type = type;
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.primaryKeys = primaryKeys;
    this.columns = columns;
  }

    public String columnList(String alias) {
    List<String> columnList =
        columns.stream()
            .map(
                column -> {
                  if (alias == null || alias.equals("")) {
                    return StreamlinerUtil.quoteIdentifierIfNeeded(column.getDestinationName());
                  } else {
                    return String.format(
                        "%s.%s",
                        alias,
                        StreamlinerUtil.quoteIdentifierIfNeeded(column.getDestinationName()));
                  }
                })
            .collect(Collectors.toList());
    return StringUtils.join(columnList, ",");
  }

  public String pkConstraint(String aAlias, String bAlias, String joinCondition) {
    List<String> pkList =
        primaryKeys.stream()
            .map(pk -> String.format("%s.%s = %s.%s", aAlias, pk, bAlias, pk))
            .collect(Collectors.toList());
    return StringUtils.join(pkList, joinCondition == null ? " AND " : joinCondition);
  }

  public String columnConstraint(String aAlias, String bAlias, String joinCondition) {
    List<String> columnList =
        columns.stream()
            .map(
                column -> {
                  if (aAlias == null || aAlias.equals("")) {
                    return String.format(
                        "%s = %s.%s",
                        column.getDestinationName(), bAlias, column.getDestinationName());
                  } else {
                    return String.format(
                        "%s.%s = %s.%s",
                        aAlias,
                        StreamlinerUtil.quoteIdentifierIfNeeded(column.getDestinationName()),
                        bAlias,
                        StreamlinerUtil.quoteIdentifierIfNeeded(column.getDestinationName()));
                  }
                })
            .collect(Collectors.toList());

    return StringUtils.join(columnList, joinCondition == null ? " AND " : joinCondition);
  }
}
