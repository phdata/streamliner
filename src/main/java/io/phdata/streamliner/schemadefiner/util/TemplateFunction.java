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
package io.phdata.streamliner.schemadefiner.util;

import io.phdata.streamliner.schemadefiner.model.ColumnDefinition;
import io.phdata.streamliner.schemadefiner.model.HadoopTable;
import io.phdata.streamliner.util.JavaHelper;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class TemplateFunction {

  static final String[] zeroScaleSuffixes = {
    "_KEY",
    "_ID",
    "_NUM",
    "_CNUM",
    "_COUNT",
    "_COUNTER",
    "_CNT",
    "_DAY",
    "_DAY1",
    "_DAY2",
    "_WK",
    "_MTH",
    "_QTR",
    "_YR",
    "_PRECEDENCE",
    "_OFFSET",
    "_REL",
    "_SEQ",
    "_TS",
    "_AGO",
    "_ITEM",
    "_JLN"
  };

  public static String mapDataType(
      ColumnDefinition column,
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping,
      String storageFormat) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);
    ColumnDefinition cColumn = normalizeColumnDefinition(column, storageFormat);
    Map<String, String> dataTypeMap = javaTypeMap.get(cColumn.getDataType().toLowerCase());

    if (dataTypeMap == null || dataTypeMap.isEmpty()) {
      throw new RuntimeException(
          String.format(
              "No type mapping found for data type: '%s' in provided type mapping file",
              column.getDataType()));
    }
    String dataType = dataTypeMap.get(storageFormat.toLowerCase());
    if (dataType == null) {
      throw new RuntimeException(
          String.format(
              "No type mapping found for data type: '%s' and storage format: %s in provided type mapping",
              column.getDataType(), storageFormat));
    }
    return dataType;
  }

  private static ColumnDefinition checkOracleNumberType(
      ColumnDefinition columnDefinition, String format) {

    int precision = columnDefinition.getPrecision() == null ? 0 : columnDefinition.getPrecision();
    int scale = columnDefinition.getScale() == null ? 0 : columnDefinition.getScale();

    if (columnDefinition.getDataType().equalsIgnoreCase("NUMBER")) {
      if (scale > 0) {
        columnDefinition.setDataType("DECIMAL");
      } else if (precision > 19 && scale == 0) {
        columnDefinition.setDataType("DECIMAL");
      } else if (precision >= 10 && precision <= 19 && scale == 0) {
        columnDefinition.setDataType("BIGINT");
      } else if (precision == 0 && scale == -127 && !format.equalsIgnoreCase("SNOWFLAKE")) {
        columnDefinition.setDataType("VARCHAR");
      } else if (precision == 0 && scale == -127 && format.equalsIgnoreCase("SNOWFLAKE")) {
        columnDefinition.setDataType("NUMBER");
      } else {
        columnDefinition.setDataType("INTEGER");
      }
    } else {
      return columnDefinition;
    }
    return columnDefinition;
  }

  private static ColumnDefinition normalizeColumnDefinition(
      ColumnDefinition columnDefinition, String format) {
    columnDefinition.setDataType(stripSuffix(columnDefinition.getDataType()));
    return checkOracleNumberType(columnDefinition, format);
  }

  private static String stripSuffix(String dataType) {
    if (!dataType.toUpperCase().endsWith(" IDENTITY")) {
      return dataType.toUpperCase();
    } else {
      int index = dataType.toUpperCase().lastIndexOf(" IDENTITY");
      return dataType.substring(0, index);
    }
  }

  public static Boolean isZeroScaleSuffix(ColumnDefinition column) {
    return column.getDataType().equalsIgnoreCase("NUMBER")
        && column.getPrecision() == 0
        && (column.getScale() == -127 || column.getScale() == 0)
        && Arrays.stream(zeroScaleSuffixes)
            .anyMatch(suffix -> column.getSourceName().endsWith(suffix.toUpperCase()));
  }

  public static String sqoopMapJavaColumn(HadoopTable tableDefinition) {
    List<String> map =
        tableDefinition.getColumns().stream()
            .map(
                column -> {
                  String[] stringTypes = {
                    "clob", "longvarbinary", "varbinary", "rowid", "blob", "nclob", "text", "binary"
                  };
                  String[] intTypes = {"tinyint", "int", "smallint", "integer", "short"};
                  String[] floatTypes = {"float"};
                  String[] longDataTypes = {"bigint"};

                  List<String> stringList = Arrays.asList(stringTypes);
                  List<String> intList = Arrays.asList(intTypes);
                  List<String> floatList = Arrays.asList(floatTypes);
                  List<String> longList = Arrays.asList(longDataTypes);

                  String dataType =
                      checkOracleNumberType(column, "Hadoop").getDataType().toLowerCase();
                  if (stringList.contains(dataType)) {
                    return String.format("%s=String", column.getDestinationName());
                  } else if (floatList.contains(dataType)) {
                    return String.format("%s=Float", column.getDestinationName());
                  } else if (intList.contains(dataType)) {
                    return String.format("%s=Integer", column.getDestinationName());
                  } else if (longList.contains(dataType)) {
                    return String.format("%s=Long", column.getDestinationName());
                  } else {
                    return "";
                  }
                })
            .collect(Collectors.toList());
    if (map.isEmpty()) {
      return null;
    } else {
      return StringUtils.join(map, ",");
    }
  }
}
