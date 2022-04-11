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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
    visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = HadoopTable.class, name = "Hadoop"),
  @JsonSubTypes.Type(value = SnowflakeTable.class, name = "Snowflake")
})
@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class TableDefinition {
  public String type;
  public String sourceName;
  public String destinationName;
  public List<String> primaryKeys;
  public List<ColumnDefinition> columns;

  public TableDefinition() {}

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
            .map(
                pk ->
                    String.format(
                        "%s.%s = %s.%s",
                        aAlias,
                        StreamlinerUtil.quoteIdentifierIfNeeded(pk),
                        bAlias,
                        StreamlinerUtil.quoteIdentifierIfNeeded(pk)))
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
                        StreamlinerUtil.quoteIdentifierIfNeeded(column.getDestinationName()),
                        bAlias,
                        StreamlinerUtil.quoteIdentifierIfNeeded(column.getDestinationName()));
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
