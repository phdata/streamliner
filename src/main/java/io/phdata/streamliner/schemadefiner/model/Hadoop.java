package io.phdata.streamliner.schemadefiner.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
@Getter
@Setter
public class Hadoop extends Destination {
  private String type;
  public String impalaShellCommand;
  public HadoopDatabase stagingDatabase;
  public HadoopDatabase reportingDatabase;

  public Hadoop() {}
}
