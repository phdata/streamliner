package io.phdata.streamliner.schemadefiner.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class SnowflakeQAOptions {
  public String taskSchedule = "5 minutes";
  public Double minimumPercentage;
  public Integer minimumCount;
  public Integer minimumRuns;
  public Double standardDeviations;

  public SnowflakeQAOptions() {}
}
