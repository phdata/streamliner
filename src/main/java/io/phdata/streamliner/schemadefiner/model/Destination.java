package io.phdata.streamliner.schemadefiner.model;

public class Destination {
    private String type;
    private String snowSqlCommand;
    private String storagePath;
    private String storageIntegration;
    private String warehouse;
    private String taskSchedule;
    private Database stagingDatabase;
    private Database reportingDatabase;
    private String stageName;
    private IngestConfigFileFormat fileFormat;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSnowSqlCommand() {
        return snowSqlCommand;
    }

    public void setSnowSqlCommand(String snowSqlCommand) {
        this.snowSqlCommand = snowSqlCommand;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public String getStorageIntegration() {
        return storageIntegration;
    }

    public void setStorageIntegration(String storageIntegration) {
        this.storageIntegration = storageIntegration;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getTaskSchedule() {
        return taskSchedule;
    }

    public void setTaskSchedule(String taskSchedule) {
        this.taskSchedule = taskSchedule;
    }

    public Database getStagingDatabase() {
        return stagingDatabase;
    }

    public void setStagingDatabase(Database stagingDatabase) {
        this.stagingDatabase = stagingDatabase;
    }

    public Database getReportingDatabase() {
        return reportingDatabase;
    }

    public void setReportingDatabase(Database reportingDatabase) {
        this.reportingDatabase = reportingDatabase;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    public IngestConfigFileFormat getFileFormat() {
        return fileFormat;
    }

    public void setFileFormat(IngestConfigFileFormat fileFormat) {
        this.fileFormat = fileFormat;
    }
}
