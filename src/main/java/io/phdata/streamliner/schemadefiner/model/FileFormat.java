package io.phdata.streamliner.schemadefiner.model;

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

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getNullIf() {
        return nullIf;
    }

    public void setNullIf(String nullIf) {
        this.nullIf = nullIf;
    }
}
