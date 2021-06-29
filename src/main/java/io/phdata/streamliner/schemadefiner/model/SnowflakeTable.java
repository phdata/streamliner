package io.phdata.streamliner.schemadefiner.model;

import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class SnowflakeTable extends TableDefinition {
  private String type;
  private String sourceName;
  public String destinationName;
  private String comment;
  private List<String> primaryKeys;
  private String changeColumn;
  private String incrementalTimeStamp;
  private Map<String, String> metadata;
  private FileFormat fileFormat;
  private List<ColumnDefinition> columns;

  public SnowflakeTable() {}

  public SnowflakeTable(
      String sourceName,
      String destinationName,
      String comment,
      List<String> primaryKeys,
      String changeColumn,
      String incrementalTimeStamp,
      Map<String, String> metadata,
      FileFormat fileFormat,
      List<ColumnDefinition> columns) {
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.comment = comment;
    this.primaryKeys = primaryKeys;
    this.changeColumn = changeColumn;
    this.incrementalTimeStamp = incrementalTimeStamp;
    this.metadata = metadata;
    this.fileFormat = fileFormat;
    this.columns = columns;
  }

  public String pkList = StringUtils.join(primaryKeys, ",");

  public String columnDDL(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);

    List<String> list =
        columns.stream()
            .map(
                column ->
                    String.format(
                        "%s %s COMMENT '%s'",
                        StreamlinerUtil.quoteIdentifierIfNeeded(column.getDestinationName()),
                        column.mapDataTypeSnowflake(javaTypeMap),
                        column.getComment()))
            .collect(Collectors.toList());
    return StringUtils.join(list, ",\n");
  }
}
