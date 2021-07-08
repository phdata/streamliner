package io.phdata.streamliner.schemadefiner.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class Snowflake extends Destination {
  private String type;
  public String snowSqlCommand;
  public String storagePath;
  public String storageIntegration;
  public String snsTopic;
  public String warehouse;
  public String taskSchedule = "5 minutes";
  public SnowflakeQAOptions quality;
  public SnowflakeDatabase stagingDatabase;
  public SnowflakeDatabase reportingDatabase;
  public String stageName;
  public IngestFileFormat fileFormat;

  public Snowflake() {}
}
