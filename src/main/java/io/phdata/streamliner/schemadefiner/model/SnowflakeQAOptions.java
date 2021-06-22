package io.phdata.streamliner.schemadefiner.model;

public class SnowflakeQAOptions {
  private String taskSchedule;
  private Double minimumPercentage;
  private Integer minimumCount;
  private Integer minimumRuns;
  private Double standardDeviations;

  public SnowflakeQAOptions() {}

  public String getTaskSchedule() {
    return taskSchedule;
  }

  public void setTaskSchedule(String taskSchedule) {
    this.taskSchedule = taskSchedule;
  }

  public Double getMinimumPercentage() {
    return minimumPercentage;
  }

  public void setMinimumPercentage(Double minimumPercentage) {
    this.minimumPercentage = minimumPercentage;
  }

  public Integer getMinimumCount() {
    return minimumCount;
  }

  public void setMinimumCount(Integer minimumCount) {
    this.minimumCount = minimumCount;
  }

  public Integer getMinimumRuns() {
    return minimumRuns;
  }

  public void setMinimumRuns(Integer minimumRuns) {
    this.minimumRuns = minimumRuns;
  }

  public Double getStandardDeviations() {
    return standardDeviations;
  }

  public void setStandardDeviations(Double standardDeviations) {
    this.standardDeviations = standardDeviations;
  }
}
