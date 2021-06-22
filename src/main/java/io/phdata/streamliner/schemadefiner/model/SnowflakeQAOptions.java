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
  private String taskSchedule;
  private Double minimumPercentage;
  private Integer minimumCount;
  private Integer minimumRuns;
  private Double standardDeviations;

  public SnowflakeQAOptions() {}
}
