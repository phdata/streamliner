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
}
