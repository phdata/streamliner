package io.phdata.streamliner.schemadefiner.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class FileFormat {
    private String location;
    private String fileType;
    private String delimiter;
    private String nullIf;

    public FileFormat() {
    }

    public FileFormat(String location, String fileType) {
        this.location = location;
        this.fileType = fileType;
    }

    public FileFormat(String location, String fileType, String delimiter, String nullIf) {
        this.location = location;
        this.fileType = fileType;
        this.delimiter = delimiter;
        this.nullIf = nullIf;
    }
}
