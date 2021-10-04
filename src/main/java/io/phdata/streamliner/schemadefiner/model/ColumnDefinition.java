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

import io.phdata.streamliner.util.JavaHelper;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
@Slf4j
public class ColumnDefinition {
  public static final int SNOWFLAKE_MAX_LENGTH = 16_777_216;
  public String sourceName;
  public String destinationName;
  public String dataType;
  public String comment = "";
  public Integer precision;
  public Integer scale;
  public boolean nullable = true;

  public ColumnDefinition() {}

  public ColumnDefinition(
      String sourceName,
      String destinationName,
      String dataType,
      String comment,
      Integer precision,
      Integer scale,
      boolean nullable) {
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.dataType = dataType;
    this.comment = comment;
    this.precision = precision;
    this.scale = scale;
    this.nullable = nullable;
  }

  public ColumnDefinition(
      String sourceName, String destinationName, String dataType, String comment) {
    this.sourceName = sourceName;
    this.destinationName = destinationName;
    this.dataType = dataType;
    this.comment = comment;
  }

  private String cleanseDataType(String dataType) {
    if (!dataType.endsWith(" IDENTITY")) {
      return dataType.toUpperCase();
    } else {
      int index = dataType.lastIndexOf(" IDENTITY");
      return dataType.substring(0, index);
    }
  }

  public String mapDataTypeSnowflake(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);
    return mapDataTypeSnowflake(javaTypeMap);
  }

  public String mapDataTypeSnowflake(Map<String, Map<String, String>> typeMapping) {
    String cleanDataType = cleanseDataType(dataType);
    Integer p = precision == null ? 0 : precision;
    Integer s = scale == null ? 0 : scale;

    if (p > SNOWFLAKE_MAX_LENGTH) {
      log.warn(
          "Truncating column {} (source name) from {} to Snowflakes max of {}",
          sourceName,
          p,
          SNOWFLAKE_MAX_LENGTH);
      p = SNOWFLAKE_MAX_LENGTH;
    }

    // Oracle specific type mapping
    if (cleanDataType.equalsIgnoreCase("NUMBER")) {
      if (p == 0 && (s == -127 || s == 0)) {
        return "NUMBER(38, 8)";
      } else {
        return String.format("NUMBER(%d, %d)", p, s);
      }
    } else {
      String dataType = mapDataType(cleanDataType, typeMapping, "SNOWFLAKE");
      if (dataType.equalsIgnoreCase("varchar")) {
        return String.format("VARCHAR(%d)", p);
      } else if (dataType.equalsIgnoreCase("char")) {
        return String.format("CHAR(%d)", p);
      } else {
        return dataType;
      }
    }
  }

  public String mapDataTypeHadoop(
      Map<String, Map<String, String>> typeMapping, String targetFormat) {
    return getHadoopDataType(targetFormat, typeMapping);
  }

  public String mapDataTypeHadoop(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping,
      String targetFormat) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);
    return getHadoopDataType(targetFormat, javaTypeMap);
  }

  private String getHadoopDataType(
      String targetFormat, Map<String, Map<String, String>> javaTypeMap) {
    String cleanDataType = cleanseDataType(dataType);
    Integer p = precision == null ? 0 : precision;
    Integer s = scale == null ? 0 : scale;

    // Oracle specific type mapping
    if (cleanDataType.equalsIgnoreCase("NUMBER")) {
      if (s > 0) {
        return String.format("DECIMAL(%d, %d)", p, s);
      } else if (p > 19 && s == 0) {
        return String.format("DECIMAL(%d, %d)", p, s);
      } else if (p >= 10 && p <= 19 && s == 0) {
        return "BIGINT";
      } else if (p == 0 && s == -127) {
        return "VARCHAR";
      } else {
        return "INTEGER";
      }
    } else if (cleanDataType.equalsIgnoreCase("DECIMAL")) {
      return String.format("DECIMAL(%d, %d)", p, s);
    } else {
      return mapDataType(cleanDataType, javaTypeMap, targetFormat);
    }
  }

  public String mapDataType(
      String sourceDataType, Map<String, Map<String, String>> typeMapping, String targetFormat) {
    Map<String, String> dataTypeMap = typeMapping.get(sourceDataType.toLowerCase());
    if (dataTypeMap == null || dataTypeMap.isEmpty()) {
      throw new RuntimeException(
          String.format(
              "No type mapping found for data type: '%s' in provided type mapping file",
              sourceDataType));
    }
    String dataType = dataTypeMap.get(targetFormat.toLowerCase());
    if (dataType == null) {
      throw new RuntimeException(
          String.format(
              "No type mapping found for data type: '%s' and storage format: %s in provided type mapping",
              dataType, targetFormat));
    }
    return dataType;
  }

  public String mapDataTypeJava(Map<String, Map<String, String>> typeMapping) {
    String[] strings = {
      "clob", "longvarbinary", "varbinary", "rowid", "blob", "nclob", "text", "binary"
    };
    String[] ints = {"tinyint", "int", "smallint", "integer", "short"};

    List<String> stringList = Arrays.asList(strings);
    List<String> intList = Arrays.asList(ints);

    String dataType = mapDataTypeHadoop(typeMapping, "AVRO").toLowerCase();
    if (stringList.contains(dataType)) {
      return String.format("%s=String", destinationName);
    } else if (intList.contains(dataType)) {
      return String.format("%s=Integer", destinationName);
    } else if (dataType.equalsIgnoreCase("float")) {
      return String.format("%s=Float", destinationName);
    } else if (dataType.equalsIgnoreCase("bigint")) {
      return String.format("%s=Long", destinationName);
    } else {
      return "";
    }
  }

  public String castColumn(
      Map<String, Map<String, String>> typeMapping, String sourceFormat, String targetFormat) {
    String sourceDataType = mapDataTypeHadoop(typeMapping, sourceFormat);
    String targetDataType = mapDataTypeHadoop(typeMapping, targetFormat);

    if (sourceDataType.equalsIgnoreCase(targetDataType)) {
      return String.format("`%s`", destinationName);
    } else {
      return String.format(
          "CAST(`%s` AS %s) AS `%s`", destinationName, targetDataType, destinationName);
    }
  }
}
