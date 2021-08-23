package io.phdata.streamliner.schemadefiner.model;

import com.opencsv.bean.CsvBindByPosition;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@Getter
@Setter
public class CSVColumns {

    @CsvBindByPosition(position = 0)
    private String tableName;

    @CsvBindByPosition(position = 1)
    private String columnName;

    @CsvBindByPosition(position = 2)
    private String dataType;

    @CsvBindByPosition(position = 3)
    private String isNullable;

    @CsvBindByPosition(position = 4)
    private String comment;

    @CsvBindByPosition(position = 5)
    private String ordinalPosition;

    @CsvBindByPosition(position = 6)
    private String precision;

    @CsvBindByPosition(position = 7)
    private String scale;

}
