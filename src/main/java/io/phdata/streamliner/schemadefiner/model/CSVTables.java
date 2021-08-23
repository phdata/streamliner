package io.phdata.streamliner.schemadefiner.model;

import com.opencsv.bean.CsvBindByPosition;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@Getter
@Setter
public class CSVTables {

    @CsvBindByPosition(position = 0)
    private String tableSchema;

    @CsvBindByPosition(position = 1)
    private String tableName;

    @CsvBindByPosition(position = 2)
    private String tableType;

    @CsvBindByPosition(position = 3)
    private String tableComment;


}
