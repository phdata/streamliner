// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package io.phdata.streamliner.schemadefiner.model;

import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class SnowflakeTable extends TableDefinition {
  private String type;
  public String sourceName;
  public String destinationName;
  public String comment = "";
  private List<String> primaryKeys;
  private String changeColumn;
  private String incrementalTimeStamp;
  private Map<String, String> metadata;
  private FileFormat fileFormat;
  public List<ColumnDefinition> columns;
  public String pkList;

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
    pkList =
        StringUtils.join(
            primaryKeys.stream()
                .map(c -> StreamlinerUtil.quoteIdentifierIfNeeded(c))
                .collect(Collectors.toList()),
            ",");
  }

  // these setters are needed to set value to parent class. jackson is not setting the parent class
  // value
  public void setDestinationName(String destinationName) {
    super.destinationName = destinationName;
    this.destinationName = destinationName;
  }

  public void setSourceName(String sourceName) {
    super.setSourceName(sourceName);
    this.sourceName = sourceName;
  }

  public void setPrimaryKeys(List<String> primaryKeys) {
    super.setPrimaryKeys(primaryKeys);
    this.primaryKeys = primaryKeys;
  }

  public void setColumns(List<ColumnDefinition> columns) {
    super.setColumns(columns);
    this.columns = columns;
  }

  public String columnDDL(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);

    List<String> columnList =
        columns.stream()
            .map(
                column ->
                    String.format(
                        "%s %s %s COMMENT '%s'",
                        StreamlinerUtil.quoteIdentifierIfNeeded(column.getDestinationName()),
                        column.mapDataTypeSnowflake(javaTypeMap),
                        column.isNullable() ? "" : "NOT NULL",
                        column.getComment()))
            .collect(Collectors.toList());
    return StringUtils.join(columnList, ",\n");
  }

  public String sourceColumnConversion(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);
    List<String> columnList =
        columns.stream()
            .map(
                column ->
                    String.format(
                        "$1:%s::%s",
                        StreamlinerUtil.quoteIdentifierIfNeeded(column.getSourceName()),
                        column.mapDataTypeSnowflake(javaTypeMap)))
            .collect(Collectors.toList());

    return StringUtils.join(columnList, ",\n");
  }
}
