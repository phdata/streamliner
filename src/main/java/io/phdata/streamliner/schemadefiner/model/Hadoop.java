package io.phdata.streamliner.schemadefiner.model;

public class Hadoop extends Destination {
  private String type;
  private String impalaShellCommand;
  private HadoopDatabase stagingDatabase;
  private HadoopDatabase reportingDatabase;

  public Hadoop() {}

  @Override
  public String getType() {
    return type;
  }

  @Override
  public void setType(String type) {
    this.type = type;
  }

  public String getImpalaShellCommand() {
    return impalaShellCommand;
  }

  public void setImpalaShellCommand(String impalaShellCommand) {
    this.impalaShellCommand = impalaShellCommand;
  }

  public HadoopDatabase getStagingDatabase() {
    return stagingDatabase;
  }

  public void setStagingDatabase(HadoopDatabase stagingDatabase) {
    this.stagingDatabase = stagingDatabase;
  }

  public HadoopDatabase getReportingDatabase() {
    return reportingDatabase;
  }

  public void setReportingDatabase(HadoopDatabase reportingDatabase) {
    this.reportingDatabase = reportingDatabase;
  }
}
