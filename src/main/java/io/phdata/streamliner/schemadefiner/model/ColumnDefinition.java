package io.phdata.streamliner.schemadefiner.model;

import io.phdata.streamliner.schemadefiner.util.StreamlinerUtil;
import io.phdata.streamliner.util.JavaHelper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class ColumnDefinition {
    private String sourceName;
    private String destinationName;
    private String dataType;
    private String comment;
    private Integer precision;
    private Integer scale;

    public ColumnDefinition() {
    }

    public ColumnDefinition(String sourceName, String destinationName, String dataType, String comment, Integer precision, Integer scale) {
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.dataType = dataType;
        this.comment = comment;
        this.precision = precision;
        this.scale = scale;
    }

    public ColumnDefinition(String sourceName, String destinationName, String dataType, String comment) {
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.dataType = dataType;
        this.comment = comment;
    }

    private String cleanseDataType(String dataType){
        if(!dataType.endsWith(" IDENTITY")){
            return dataType.toUpperCase();
        }else{
            int index = dataType.lastIndexOf(" IDENTITY");
            return dataType.substring(0, index);
        }
    }

  public String mapDataTypeSnowflake(
      scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>
          typeMapping) {
    Map<String, Map<String, String>> javaTypeMap = JavaHelper.convertScalaMapToJavaMap(typeMapping);
    String cleanDataType = cleanseDataType(dataType);
    Integer p = precision == null ? 0 : precision;
    Integer s = scale == null ? 0 : scale;

    if (cleanDataType.equalsIgnoreCase("NUMBER")) {
      if (p == 0 && (s == -127 || s == 0)) {
        return "NUMBER(38, 8)";
      } else {
        return String.format("NUMBER(%d, %d)", p, s);
      }
    } else {
      String dataType = StreamlinerUtil.mapDataType(cleanDataType, javaTypeMap, "SNOWFLAKE");
      if (dataType.equalsIgnoreCase("varchar")) {
        return String.format("VARCHAR(%d)", p);
      } else if (dataType.equalsIgnoreCase("char")) {
        return String.format("CHAR($d)", p);
      } else {
        return dataType;
      }
    }
  }

  public String mapDataTypeSnowflake(Map<String, Map<String, String>> typeMapping) {
    String cleanDataType = cleanseDataType(dataType);
    Integer p = precision == null ? 0 : precision;
    Integer s = scale == null ? 0 : scale;

    if (cleanDataType.equalsIgnoreCase("NUMBER")) {
      if (p == 0 && (s == -127 || s == 0)) {
        return "NUMBER(38, 8)";
      } else {
        return String.format("NUMBER(%d, %d)", p, s);
      }
    } else {
      String dataType = StreamlinerUtil.mapDataType(cleanDataType, typeMapping, "SNOWFLAKE");
      if (dataType.equalsIgnoreCase("varchar")) {
        return String.format("VARCHAR(%d)", p);
      } else if (dataType.equalsIgnoreCase("char")) {
        return String.format("CHAR($d)", p);
      } else {
        return dataType;
      }
    }
  }
}
