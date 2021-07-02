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
  private String storagePath;
  private String storageIntegration;
  private String snsTopic;
  private String warehouse;
  private String taskSchedule;
  private SnowflakeQAOptions quality;
  public SnowflakeDatabase stagingDatabase;
  private SnowflakeDatabase reportingDatabase;

  public Snowflake() {}
}
