package io.phdata.streamliner.schemadefiner.model;

public class Snowflake extends Destination {
  private String type;
  private String snowSqlCommand;
  private String storagePath;
  private String storageIntegration;
  private String snsTopic;
  private String warehouse;
  private String taskSchedule;
  private SnowflakeQAOptions quality;
  private SnowflakeDatabase stagingDatabase;
  private SnowflakeDatabase reportingDatabase;

  public Snowflake() {}

  @Override
  public String getType() {
    return type;
  }

  @Override
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

  public String getSnsTopic() {
    return snsTopic;
  }

  public void setSnsTopic(String snsTopic) {
    this.snsTopic = snsTopic;
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

  public SnowflakeQAOptions getQuality() {
    return quality;
  }

  public void setQuality(SnowflakeQAOptions quality) {
    this.quality = quality;
  }

  public SnowflakeDatabase getStagingDatabase() {
    return stagingDatabase;
  }

  public void setStagingDatabase(SnowflakeDatabase stagingDatabase) {
    this.stagingDatabase = stagingDatabase;
  }

  public SnowflakeDatabase getReportingDatabase() {
    return reportingDatabase;
  }

  public void setReportingDatabase(SnowflakeDatabase reportingDatabase) {
    this.reportingDatabase = reportingDatabase;
  }
}
